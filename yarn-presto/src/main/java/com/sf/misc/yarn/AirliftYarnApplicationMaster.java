package com.sf.misc.yarn;

import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import com.sf.misc.airlift.Airlift;
import com.sf.misc.airlift.AirliftConfig;
import com.sf.misc.async.ListenablePromise;
import com.sf.misc.async.Promises;
import com.sf.misc.yarn.launcher.ContainerLauncher;
import com.sf.misc.yarn.launcher.LauncherEnviroment;
import com.sf.misc.yarn.rediscovery.YarnRediscovery;
import com.sf.misc.yarn.rediscovery.YarnRediscoveryModule;
import com.sf.misc.yarn.rpc.YarnRMProtocol;
import com.sf.misc.yarn.rpc.YarnRMProtocolConfig;
import io.airlift.discovery.client.ServiceInventoryConfig;
import io.airlift.log.Logger;
import org.apache.hadoop.io.retry.RetryInvocationHandler;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SaslRpcClient;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.security.AMRMTokenSelector;
import org.apache.hadoop.yarn.security.NMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.File;
import java.io.FileWriter;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AirliftYarnApplicationMaster {

    public static final Logger LOGGER = Logger.get(AirliftYarnApplicationMaster.class);

    protected final ListenablePromise<ContainerConfiguration> configuration;
    protected final ListenablePromise<Airlift> airlift;
    protected final ListenablePromise<ContainerLauncher> launcher;
    protected final String container_id;

    public AirliftYarnApplicationMaster(Map<String, String> system_enviroment) {
        configuration = recoverConfig(system_enviroment);
        container_id = system_enviroment.get(ApplicationConstants.Environment.CONTAINER_ID.key());

        // start sertices
        airlift = configuration.transformAsync((config) -> createAirlift(config));

        launcher = configuration.transformAsync((config) -> createContainerLauncer(config));
    }

    public ListenablePromise<ContainerConfiguration> configuration() {
        return this.configuration;
    }

    public ListenablePromise<Airlift> getAirlift() {
        return airlift;
    }

    public ListenablePromise<ContainerLauncher> launcher() {
        return this.launcher;
    }

    protected ListenablePromise<ContainerConfiguration> recoverConfig(Map<String, String> envs) {
        return Promises.submit(() -> {
            envs.entrySet().parallelStream().forEach((entry) -> {
                LOGGER.info("env key:" + entry.getKey() + " value:" + entry.getValue());
            });

            ContainerConfiguration configuration = ContainerConfiguration.decode(envs.get(ContainerConfiguration.class.getName()));
            configuration.configs().entrySet()
                    .parallelStream()
                    .forEach((entry) -> {
                        LOGGER.info("context config key:" + entry.getKey() + " value:" + entry.getValue());
                    });
            return configuration;
        });
    }

    protected ListenablePromise<YarnRMProtocol> createRMProtocol(YarnRMProtocolConfig config) {
        // create protocol
        ListenablePromise<YarnRMProtocol> protocol = YarnRMProtocol.create(config);
        ListenablePromise<URI> inventory = airlift.transform((airlift) -> {
            return airlift.getInstance(ServiceInventoryConfig.class).getServiceInventoryUri();
        });

        return Promises.<YarnRMProtocol, URI, YarnRMProtocol>chain(YarnRMProtocol.create(config), inventory) //
                .call((master, inventory_uri) -> {
                    // then register application with inventory uri
                    RegisterApplicationMasterResponse response = master.registerApplicationMaster( //
                            RegisterApplicationMasterRequest.newInstance( //
                                    inventory_uri.getHost(), //
                                    inventory_uri.getPort(), //
                                    inventory_uri.toURL().toExternalForm() //
                            ) //
                    );

                    // add tokens back if any
                    response.getNMTokensFromPreviousAttempts().parallelStream().forEach((token) -> {
                        LOGGER.info("add previrouse attempt nodemanager token..." + token.getNodeId());

                        org.apache.hadoop.security.token.Token<NMTokenIdentifier> nmToken =
                                ConverterUtils.convertFromYarn(token.getToken(), NetUtils.createSocketAddr(token.getNodeId().toString()));
                        master.ugi().addToken(nmToken);
                    });

                    // switch to token?
                    //master.ugi().setAuthenticationMethod(SaslRpcServer.AuthMethod.TOKEN);
                    return master;
                });
    }

    protected ListenablePromise<Airlift> createAirlift(ContainerConfiguration master_contaienr_config) {
        // setup log levels
        ListenablePromise<AirliftConfig> airlift_config = Promises.submit(() -> {
            // adjust node evn
            AirliftConfig config = inherentConfig(master_contaienr_config.distill(AirliftConfig.class));

            File log_levels = new File("airlift-log.config");
            try (FileWriter stream = new FileWriter(log_levels)) {
                for (String line : logLevels()) {
                    stream.write(line);
                    stream.write("\n");
                }
            }

            // adjust config
            config.setLoglevel(log_levels.getAbsolutePath());
            return config;
        });

        return airlift_config.transform((config) -> {
            // create airlift
            Airlift airlift = new Airlift(config) //
                    .module( // attach rediscovery module
                            new YarnRediscoveryModule( //
                                    ConverterUtils.toContainerId(container_id)
                                            .getApplicationAttemptId()
                                            .getApplicationId()
                                            .toString()
                            )
                    );

            // other module
            this.modules().stream().forEach(airlift::module);

            // start airlift
            return airlift.start().callback((ignore, throwable) -> {
                if (throwable != null) {
                    LOGGER.error(throwable, "fail to start airlift");
                    return;
                }

                // start rediscovery
                airlift.getInstance(YarnRediscovery.class).start();
            });
        }).transformAsync((through) -> through);
    }

    protected ListenablePromise<ContainerLauncher> createContainerLauncer(ContainerConfiguration container_config) {
        // preprae rm protocl
        ListenablePromise<YarnRMProtocol> protocol = createRMProtocol(container_config.distill(YarnRMProtocolConfig.class));

        // prepare laucnher enviroment
        LauncherEnviroment launcher_enviroment = new LauncherEnviroment( //
                Promises.immediate(URI.create(container_config.classloader())) //
        );

        // build launcher
        return Promises.immediate( //
                new ContainerLauncher( //
                        protocol, //
                        Promises.immediate(launcher_enviroment), //
                        false //
                ) //
        );
    }

    protected Collection<Module> modules() {
        return Collections.emptySet();
    }

    protected List<String> logLevels() {
        Stream<String> debug = Stream.of(
                //SaslRpcClient.class.getName(),
                //AMRMTokenSelector.class.getName()
        ).parallel().map((level) -> level + "=DEBUG");

        Stream<String> error = Stream.of(
                RetryInvocationHandler.class.getName()
        ).parallel().map((level) -> level + "=ERROR");

        return Stream.of(
                debug,//
                error//
        ).parallel().flatMap(Function.identity()) //
                .collect(Collectors.toList());
    }

    protected AirliftConfig inherentConfig(AirliftConfig parent_config) {
        AirliftConfig config = new AirliftConfig();

        // use env
        config.setNodeEnv(parent_config.getNodeEnv());

        // use classloader
        config.setClassloader(parent_config.getClassloader());

        // federation url
        config.setForeignDiscovery(parent_config.getForeignDiscovery());

        return config;
    }
}

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
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.ServiceInventoryConfig;
import io.airlift.log.Logger;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.security.NMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class AirliftYarnApplicationMaster {

    public static final Logger LOGGER = Logger.get(AirliftYarnApplicationMaster.class);

    protected final ListenablePromise<ContainerConfiguration> configuration;
    protected final ListenablePromise<Airlift> airlift;
    protected final ListenablePromise<ContainerLauncher> launcher;
    protected final String container_id;
    protected final File token_file;

    public AirliftYarnApplicationMaster(Map<String, String> system_enviroment) {
        configuration = recoverConfig(system_enviroment);
        container_id = system_enviroment.get(ApplicationConstants.Environment.CONTAINER_ID.key());
        token_file = new File(System.getenv().get(ApplicationConstants.CONTAINER_TOKEN_FILE_ENV_NAME));

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

        // recover tokens
        ListenablePromise<Credentials> credential = Promises.submit(() -> {
            Credentials credentials = new Credentials();
            try (DataInputStream token_steram = new DataInputStream(new FileInputStream(token_file))) {
                credentials.readTokenStorageStream(token_steram);
            }
            return credentials;
        });

        return Promises.<YarnRMProtocol, Credentials, ListenablePromise<URI>>chain(protocol, credential).call((master, tokens) -> {
            tokens.getAllTokens().forEach((token) -> {
                LOGGER.info("recover token:" + token);
            });

            // add token back
            master.ugi().addCredentials(tokens);

            // then find inventory uri
            return airlift.transform((airlift) -> {
                return airlift.getInstance(ServiceInventoryConfig.class).getServiceInventoryUri();
            });
        }).transformAsync((through) -> through) //
                .transformAsync((inventory_uri) -> {
                    // then register application with inventory uri
                    return protocol.transform((master) -> {
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
                });
    }

    protected ListenablePromise<Airlift> createAirlift(ContainerConfiguration master_contaienr_config) {
        // adjust node evn
        AirliftConfig airlift_config = inherentConfig(master_contaienr_config.distill(AirliftConfig.class));

        // setup log levels
        ListenablePromise<File> loglevel = Promises.submit(() -> {
            File log_levels = new File("airlift-log.config");
            try (FileWriter stream = new FileWriter(log_levels)) {
                for (String config : logLevels()) {
                    stream.write(config);
                    stream.write("\n");
                }
            }

            return log_levels;
        });

        return Promises.submit(() -> {
            // create airlift
            Airlift airlift = new Airlift(airlift_config) //
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
            return loglevel.transformAsync((log_file) -> {
                return airlift.start(log_file).callback((ignore, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error(throwable, "fail to start airlift");
                        return;
                    }

                    // start rediscovery
                    airlift.getInstance(YarnRediscovery.class).start();
                });
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
        return Collections.emptyList();
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

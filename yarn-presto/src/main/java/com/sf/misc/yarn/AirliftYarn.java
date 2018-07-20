package com.sf.misc.yarn;

import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import com.sf.misc.airlift.Airlift;
import com.sf.misc.airlift.AirliftConfig;
import com.sf.misc.async.ListenablePromise;
import com.sf.misc.async.Promises;
import io.airlift.discovery.client.ServiceInventory;
import io.airlift.discovery.client.ServiceInventoryConfig;
import io.airlift.log.Logger;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
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
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

public class AirliftYarn {

    public static final Logger LOGGER = Logger.get(AirliftYarn.class);

    protected final ListenablePromise<ContainerConfiguration> configuration;
    protected final ListenablePromise<Airlift> airlift;
    protected final ListenablePromise<YarnRMProtocol> protocol;
    protected final ListenablePromise<UserGroupInformation> ugi;

    public AirliftYarn(Map<String, String> envs) {
        configuration = recoverConfig(envs);

        // start sertices
        airlift = configuration.transformAsync((config) -> createAirlift(config, envs));
        protocol = configuration.transformAsync((config) -> createProtocol(config.distill(YarnRMProtocolConfig.class)));

        // should be last?
        ugi = createUGI();
    }

    public ListenablePromise<ContainerConfiguration> configuration() {
        return this.configuration;
    }

    public ListenablePromise<Airlift> getAirlift() {
        return airlift;
    }

    public ListenablePromise<YarnRMProtocol> getProtocol() {
        return protocol;
    }

    public ListenablePromise<UserGroupInformation> getUgi() {
        return ugi;
    }

    protected ListenablePromise<ContainerConfiguration> recoverConfig(Map<String, String> envs) {
        return Promises.submit(() -> {
            envs.entrySet().parallelStream().forEach((entry) -> {
                LOGGER.info("env key:" + entry.getKey() + " value:" + entry.getValue());
            });

            ContainerConfiguration configuration = ContainerConfiguration.recover(envs.get(ContainerConfiguration.class.getName()));
            configuration.configs().entrySet()
                    .parallelStream()
                    .forEach((entry) -> {
                        LOGGER.info("context config key:" + entry.getKey() + " value:" + entry.getValue());
                    });
            return configuration;
        });
    }

    protected ListenablePromise<UserGroupInformation> createUGI() {
        return airlift.transform((inventory) -> {
            return inventory.getInstance(ServiceInventoryConfig.class).getServiceInventoryUri();
        }).transformAsync((server_info) -> {
            return protocol.transform((master) -> {
                RegisterApplicationMasterResponse response = master.registerApplicationMaster( //
                        RegisterApplicationMasterRequest.newInstance( //
                                server_info.getHost(), //
                                server_info.getPort(), //
                                server_info.toURL().toExternalForm() //
                        ) //
                );

                response.getNMTokensFromPreviousAttempts().parallelStream().forEach((token) -> {
                    LOGGER.info("add previrouse attempt nodemanager token..." + token.getNodeId());

                    org.apache.hadoop.security.token.Token<NMTokenIdentifier> nmToken =
                            ConverterUtils.convertFromYarn(token.getToken(), NetUtils.createSocketAddr(token.getNodeId().toString()));
                    master.ugi().addToken(nmToken);
                });

                return master.ugi();
            });
        });
    }

    protected ListenablePromise<YarnRMProtocol> createProtocol(YarnRMProtocolConfig config) {
        ListenablePromise<YarnRMProtocol> protocol = YarnRMProtocol.create(config);

        ListenablePromise<Credentials> credential = Promises.submit(() -> {
            Credentials credentials = new Credentials();
            credentials.readTokenStorageStream(new DataInputStream(new FileInputStream(new File(System.getenv().get(ApplicationConstants.CONTAINER_TOKEN_FILE_ENV_NAME)))));
            return credentials;
        });

        return Promises.<YarnRMProtocol, Credentials, YarnRMProtocol>chain(protocol, credential).call((master, tokens) -> {
            master.ugi().addCredentials(tokens);
            return master;
        });
    }

    protected ListenablePromise<Airlift> createAirlift(ContainerConfiguration app_config, Map<String, String> envs) {
        AirliftConfig parent_airlift = app_config.distill(AirliftConfig.class);

        // start airlift
        AirliftConfig airlift_config = new AirliftConfig();
        airlift_config.setNodeEnv(parent_airlift.getNodeEnv());

        return Promises.submit(() -> {
            File log_levels = new File("airlift-log.config");
            try (FileWriter stream = new FileWriter(log_levels)) {
                for (String config : ImmutableList.<String>of( //
                        // log levels
                )) {
                    stream.write(config);
                    stream.write("\n");
                }
            }

            return log_levels;
        }).transformAsync((file) -> {

            String container_id = envs.get(ApplicationConstants.Environment.CONTAINER_ID.key());
            Airlift airlift = new Airlift(airlift_config) //
                    .module( //
                            YarnRediscoveryModule.createNew( //
                                    ConverterUtils.toContainerId(container_id)
                                            .getApplicationAttemptId()
                                            .getApplicationId()
                                            .toString())
                    );

            // other
            this.modules().stream().forEach(airlift::module);
            return airlift.start(file);
        });
    }

    protected Collection<Module> modules() {
        return Collections.emptySet();
    }
}

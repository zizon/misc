package com.sf.misc.yarn;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.sf.misc.airlift.Airlift;
import com.sf.misc.airlift.AirliftConfig;
import com.sf.misc.async.ListenablePromise;
import com.sf.misc.async.Promises;
import com.sf.misc.configs.ApplicationSubmitConfiguration;
import io.airlift.configuration.ConfigBinder;
import io.airlift.discovery.client.ServiceInventoryConfig;
import io.airlift.log.Logger;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.security.NMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.locks.LockSupport;

public class AirliftYarn {

    public static final Logger LOGGER = Logger.get(AirliftYarn.class);

    public static void main(String args[]) throws Throwable {
        LOGGER.info("start up...");
        Map<String, String> envs = System.getenv();
        envs.entrySet().parallelStream().forEach((entry) -> {
            LOGGER.info("env key:" + entry.getKey() + " value:" + entry.getValue());
        });

        ApplicationSubmitConfiguration configuration = ApplicationSubmitConfiguration.recover(envs.get(ApplicationSubmitConfiguration.class.getName()));
        configuration.configs().entrySet()
                .parallelStream()
                .forEach((entry) -> {
                    LOGGER.info("context config key:" + entry.getKey() + " value:" + entry.getValue());
                });
        LOGGER.info(configuration.distill(YarnRMProtocolConfig.class).getRMs());

        AirliftConfig airlift_config = configuration.distill(AirliftConfig.class);
        YarnRMProtocolConfig protocol_config = configuration.distill(YarnRMProtocolConfig.class);

        // start airlift
        ListenablePromise<Airlift> airlift = createAirlift(airlift_config);

        ListenablePromise<YarnRMProtocol> protocol = createProtocol(protocol_config);

        Promises.chain(protocol, airlift).call((master, server) -> {
            URI server_info = server.getInstance(ServiceInventoryConfig.class).getServiceInventoryUri();
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
            return master;
        });

        //


        LockSupport.park();
    }

    protected static ListenablePromise<YarnRMProtocol> createProtocol(YarnRMProtocolConfig config) {
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

    protected static ListenablePromise<Airlift> createAirlift(AirliftConfig parent_airlift) {
        // start airlift
        AirliftConfig airlift_config = new AirliftConfig();
        airlift_config.setNodeEnv(parent_airlift.getNodeEnv());

        return new Airlift(airlift_config).module(new Module() {
            @Override
            public void configure(Binder binder) {
                ConfigBinder.configBinder(binder).bindConfig(YarnApplicationConfig.class);
            }
        }).start();
    }

}

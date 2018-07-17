package com.sf.misc.yarn;

import com.sf.misc.airlift.AirliftConfig;
import com.sf.misc.configs.ApplicationSubmitConfiguration;
import com.sf.misc.presto.HiveServicesConfig;
import com.sf.misc.presto.PrestoCoordinator;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.log.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

public class TestYarnApplication {

    public static final Logger LOGGER = Logger.get(TestYarnApplication.class);

    YarnApplicationBuilder builder;

    @Before
    public void setupClient() throws Exception {
        /*
        Map<String, String> configuration = new HashMap<>();

        configuration.put("node.environment", "test");
        configuration.put("discovery.uri", "http://" + InetAddress.getLocalHost().getHostName() + ":8080");
        configuration.put("discovery.store-cache-ttl", "0s");
        configuration.put("service-inventory.uri", configuration.get("discovery.uri") + "/v1/service");
        */
        //configuration.put("service-inventory.uri", "http://" + InetAddress.getLocalHost().getHostName() + ":8080/v1/service");
        /*
        configuration.put("yarn.rms", "10.202.77.200,10.202.77.201");
        configuration.put("hdfs.nameservices", "test-cluster://10.202.77.200:8020,10.202.77.201:8020");
        configuration.put("yarn.rpc.user.real", "hive");
        configuration.put("yarn.rpc.user.proxy", "anyone");
        */

        AirliftConfig configuration = new AirliftConfig();
        //configuration.setDiscovery("http://" + InetAddress.getLocalHost().getHostName() + ":8080");
        configuration.setPort(8080);
        //configuration.setInventory(configuration.getDiscovery() + "/v1/service");
        configuration.setNodeEnv("test");
        builder = new YarnApplicationBuilder(configuration, genYarnRMProtocolConfig());
    }

    protected HiveServicesConfig genHdfsNameserviceConfig() {
        Map<String, String> configuration = new HashMap<>();
        configuration.put("hdfs.nameservices", "test-cluster://10.202.77.200:8020,10.202.77.201:8020");
        return new ConfigurationFactory(configuration).build(HiveServicesConfig.class);
    }

    protected YarnRMProtocolConfig genYarnRMProtocolConfig() {
        Map<String, String> configuration = new HashMap<>();
        configuration.put("yarn.rms", "10.202.77.200,10.202.77.201");
        configuration.put("yarn.rpc.user.proxy", "anyone");
        configuration.put("yarn.rpc.user.real", "hive");
        return new ConfigurationFactory(configuration).build(YarnRMProtocolConfig.class);
    }

    public static class TestMaster {
        public static void main(String args[]) {
            LOGGER.info("start up...");
            System.getenv().entrySet().forEach((entry) -> {
                LOGGER.info("env key:" + entry.getKey() + " value:" + entry.getValue());
            });

            ApplicationSubmitConfiguration.recover(System.getenv().get(ApplicationSubmitConfiguration.class.getName())).configs().entrySet()
                    .parallelStream()
                    .forEach((entry) -> {
                        LOGGER.info("entry key:" + entry.getKey() + " value:" + entry.getValue());
                    });
        }
    }

    @After
    public void cleanup() throws Exception {
    }

    @Test
    public void test() throws Throwable {
        ApplicationSubmitConfiguration configuration = new ApplicationSubmitConfiguration(AirliftYarn.class);
        configuration.addAirliftStyleConfig(genYarnRMProtocolConfig());
        configuration.addAirliftStyleConfig(builder.airlift.unchecked().config());
        configuration.addAirliftStyleConfig(genHdfsNameserviceConfig());

        builder.submitApplication(configuration).unchecked();
    }


}

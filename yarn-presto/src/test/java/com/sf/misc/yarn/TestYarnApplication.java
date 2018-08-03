package com.sf.misc.yarn;

import com.sf.misc.airlift.AirliftConfig;
import com.sf.misc.presto.AirliftPresto;
import com.sf.misc.presto.plugins.hive.HiveServicesConfig;
import com.sf.misc.yarn.rpc.YarnRMProtocolConfig;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.log.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.LockSupport;

public class TestYarnApplication {

    public static final Logger LOGGER = Logger.get(TestYarnApplication.class);

    YarnApplicationBuilder builder;

    @Before
    public void setupClient() throws Exception {
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
        configuration.put("hive.metastore.uri", "thrift://10.202.77.200:9083");
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

            ContainerConfiguration.decode(System.getenv().get(ContainerConfiguration.class.getName())).configs().entrySet()
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
        ContainerConfiguration container_config = new ContainerConfiguration(AirliftPresto.class, 1, 128, null);
        container_config.addAirliftStyleConfig(genYarnRMProtocolConfig());
        container_config.addAirliftStyleConfig(genHdfsNameserviceConfig());

        builder.submitApplication(container_config).unchecked();

        LOGGER.info("submited");

        LockSupport.park();
    }


}

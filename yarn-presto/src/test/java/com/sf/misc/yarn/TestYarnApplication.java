package com.sf.misc.yarn;

import com.sf.misc.airlift.AirliftConfig;
import com.sf.misc.async.Entrys;
import com.sf.misc.async.ListenablePromise;
import com.sf.misc.presto.PrestoClusterConfig;
import com.sf.misc.presto.plugins.hive.HiveServicesConfig;
import com.sf.misc.yarn.launcher.LauncherEnviroment;
import com.sf.misc.yarn.rpc.YarnRMProtocolConfig;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.log.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestYarnApplication {

    public static final Logger LOGGER = Logger.get(TestYarnApplication.class);

    YarnApplicationBuilder builder;
    Map<String, String> log_levels;

    @Before
    public void setupClient() throws Exception {
        AirliftConfig configuration = new AirliftConfig();
        //configuration.setDiscovery("http://" + InetAddress.getLocalHost().getHostName() + ":8080");
        configuration.setPort(8080);
        //configuration.setInventory(configuration.getDiscovery() + "/v1/service");
        configuration.setNodeEnv("test");

        File log_file = new File( //
                Thread.currentThread().getContextClassLoader()//
                        .getResource("airlift-log.properties") //
                        .toURI() //
        );
        try (FileInputStream stream = new FileInputStream(log_file)) {
            Properties properties = new Properties();
            properties.load(stream);

            log_levels = properties.entrySet().parallelStream()
                    .map((entry) -> Entrys.newImmutableEntry(entry.getKey().toString(), entry.getValue().toString()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        configuration.setLoglevelFile(log_file.getAbsolutePath());
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
        configuration.put("yarn.rpc.user.proxy", "anyone-exclude");
        configuration.put("yarn.rpc.user.real", "hive");
        return new ConfigurationFactory(configuration).build(YarnRMProtocolConfig.class);
    }

    public static class TestMaster {
        public static void main(String args[]) {
            LOGGER.info("start up...");
            System.getenv().entrySet().forEach((entry) -> {
                LOGGER.info("env key:" + entry.getKey() + " value:" + entry.getValue());
            });

            ContainerConfiguration.decode(LauncherEnviroment.CONTAINER_CONFIGURATION).configs().entrySet()
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
        ContainerConfiguration container_config = new PrestoClusterConfig.Builder()
                .setClusterName("presto-cluster")
                // coordinator
                .setCoordinatorMemory(512)
                .setCoordinatorCpu(1)
                // workers
                .setNumOfWorker(5)
                .setWorkerMemeory(512)
                .setWorkerCpu(1)
                .setLogLevels(log_levels)
                .buildMasterContainerConfig();

        container_config.addContextConfig(genYarnRMProtocolConfig());
        container_config.addContextConfig(genHdfsNameserviceConfig());

        // start two cluster
        Stream.of(
                //builder.submitApplication(container_config,"default"),
                builder.submitApplication(container_config,"dw")
                //builder.submitApplication(container_config)
        ).parallel().forEach(ListenablePromise::unchecked);

        LOGGER.info("submited");

        LockSupport.park();
    }


}

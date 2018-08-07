package com.sf.misc.presto;

import com.sf.misc.airlift.AirliftConfig;
import com.sf.misc.async.ListenablePromise;
import com.sf.misc.async.Promises;
import com.sf.misc.presto.plugins.hive.HiveServicesConfig;
import com.sf.misc.yarn.AirliftYarnApplicationMaster;
import com.sf.misc.yarn.ContainerConfiguration;
import com.sf.misc.yarn.launcher.ContainerLauncher;
import com.sf.misc.deprecated.YarnApplicationConfig;
import io.airlift.log.Logger;
import org.apache.hadoop.yarn.api.records.Container;

import java.io.File;
import java.util.Map;
import java.util.concurrent.locks.LockSupport;

public class AirliftPresto {

    public static final Logger LOGGER = Logger.get(AirliftPresto.class);


    protected static File INSTALLED_PLUGIN_DIR = new File("plugin");
    protected static File CATALOG_CONFIG_DIR = new File("etc/catalog/");
    protected static File PASSWORD_AUTHENTICATOR_CONFIG = new File("etc/password-authenticator.properties");
    protected static File ACCESS_CONTORL_CONFIG = new File("etc/access-control.properties");

    protected final ListenablePromise<AirliftYarnApplicationMaster> airlift_yarn_master;

    public AirliftPresto(Map<String, String> envs) {
        this.airlift_yarn_master = createAirliftYarnApplicationMaster(envs);
    }

    protected ListenablePromise<AirliftYarnApplicationMaster> createAirliftYarnApplicationMaster(Map<String, String> envs) {
        return Promises.submit(() -> {
            return new AirliftYarnApplicationMaster(envs);
        });
    }

    public ListenablePromise<Container> launchCoordinator(int memory) {
        return launcher().transformAsync((launcher) -> {
            PrestoContainerConfig config = new PrestoContainerConfig();
            config.setCoordinator(true);
            config.setMemory(memory);

            return genPrestoContaienrConfig(config).transformAsync((container_config) -> {
                return launcher.launchContainer(container_config);
            });
        });
    }

    protected ListenablePromise<ContainerLauncher> launcher() {
        return airlift_yarn_master.transformAsync((yarn) -> yarn.launcher());
    }

    protected ListenablePromise<AirliftConfig> airliftConfig() {
        return airlift_yarn_master.transformAsync((yarn) -> yarn.getAirlift()) //
                .transform((arilift) -> arilift.config());
    }

    protected ListenablePromise<ContainerConfiguration> containerConfig() {
        return airlift_yarn_master.transformAsync((airlift) -> airlift.configuration());
    }

    protected ListenablePromise<ContainerConfiguration> genPrestoContaienrConfig(PrestoContainerConfig presto_config) {
        return airliftConfig().transformAsync((config) -> {
            // prepare container config
            ContainerConfiguration configuration = new ContainerConfiguration(PrestoContainer.class, 1, presto_config.getMemory(), config.getClassloader());

            // prepare presto config
            configuration.addAirliftStyleConfig(presto_config);

            // prepare airlift
            AirliftConfig airlift_config = inherentConfig(config);
            configuration.addAirliftStyleConfig(airlift_config);

            // parepare hive service config
            return containerConfig().transform((container_config) -> {
                configuration.addAirliftStyleConfig(container_config.distill(HiveServicesConfig.class));
                return configuration;
            });
        });
    }

    protected AirliftConfig inherentConfig(AirliftConfig parent_config) {
        AirliftConfig config = new AirliftConfig();

        // use env
        config.setNodeEnv(parent_config.getNodeEnv());

        // use classloader
        config.setClassloader(parent_config.getClassloader());

        // inventory url
        config.setInventory(parent_config.getInventory());

        return config;
    }

    public static void main(String args[]) throws Throwable {
        AirliftPresto presto = new AirliftPresto(System.getenv());

        // start coordinator
        presto.launchCoordinator(512).logException();

        LockSupport.park();
    }
}

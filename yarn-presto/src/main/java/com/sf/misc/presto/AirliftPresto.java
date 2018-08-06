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

    protected final ListenablePromise<AirliftYarnApplicationMaster> airlift_yarn;

    public AirliftPresto(Map<String, String> envs) {
        this.airlift_yarn = createAirliftYarn(envs);
    }

    protected ListenablePromise<AirliftYarnApplicationMaster> createAirliftYarn(Map<String, String> envs) {
        return Promises.submit(() -> {
            return new AirliftYarnApplicationMaster(envs);
        });
    }

    public ListenablePromise<Container> launchCoordinator() {
        return launcher().transformAsync((launcher) -> {
            return genPrestoContaienrConfig(true).transformAsync((config) -> {
                return launcher.launchContainer(config);
            });
        });
    }

    protected ListenablePromise<ContainerLauncher> launcher() {
        return airlift_yarn.transformAsync((yarn) -> yarn.launcher());
    }

    protected ListenablePromise<ContainerConfiguration> genPrestoContaienrConfig(boolean coordinator) {
        PrestoContainerConfig presto = new PrestoContainerConfig();
        presto.setCoordinator(coordinator);

        ContainerConfiguration configuration = new ContainerConfiguration(PrestoContainer.class, 1, 128, null);
        configuration.addAirliftStyleConfig(presto);

        return airliftConfig().transform((config) -> configuration.addAirliftStyleConfig(config));
    }

    protected ListenablePromise<AirliftConfig> airliftConfig() {
        return airlift_yarn.transformAsync((yarn) -> yarn.getAirlift()).transform((arilift) -> arilift.config());
    }


    public static void main(String args[]) throws Throwable {
        AirliftPresto presto = new AirliftPresto(System.getenv());

        // start coordinator
        presto.launchCoordinator().logException();

        LockSupport.park();
    }

    protected static ListenablePromise<Container> launcherCoordinator(ContainerLauncher launcher, ContainerConfiguration container_config, AirliftConfig airlif_config) {
        ContainerConfiguration configuration = new ContainerConfiguration(PrestoContainer.class, 1, 128, null);
        configuration.addAirliftStyleConfig(airlif_config);

        PrestoContainerConfig presto_container_config = new PrestoContainerConfig();
        presto_container_config.setCoordinator(true);
        configuration.addAirliftStyleConfig(presto_container_config);

        // deliver more config
        configuration.addAirliftStyleConfig(container_config.distill(YarnApplicationConfig.class));
        configuration.addAirliftStyleConfig(container_config.distill(HiveServicesConfig.class));

        return launcher.launchContainer(configuration);
    }
}

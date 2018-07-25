package com.sf.misc.presto;

import com.facebook.presto.server.ServerMainModule;
import com.google.common.collect.ImmutableMap;
import com.sf.misc.airlift.AirliftConfig;
import com.sf.misc.airlift.UnionDiscoveryConfig;
import com.sf.misc.async.ListenablePromise;
import com.sf.misc.async.Promises;
import com.sf.misc.presto.plugins.hive.HiveServicesConfig;
import com.sf.misc.yarn.AirliftYarn;
import com.sf.misc.yarn.launcher.ConfigurationGenerator;
import com.sf.misc.yarn.ContainerConfiguration;
import com.sf.misc.yarn.launcher.ContainerLauncher;
import com.sf.misc.deprecated.YarnApplicationConfig;
import io.airlift.log.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Resource;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.LockSupport;

public class AirliftPresto {

    public static final Logger LOGGER = Logger.get(AirliftPresto.class);


    protected static File INSTALLED_PLUGIN_DIR = new File("plugin");
    protected static File CATALOG_CONFIG_DIR = new File("etc/catalog/");
    protected static File PASSWORD_AUTHENTICATOR_CONFIG = new File("etc/password-authenticator.properties");
    protected static File ACCESS_CONTORL_CONFIG = new File("etc/access-control.properties");

    protected final ListenablePromise<AirliftYarn> airlift_yarn;

    public AirliftPresto(Map<String, String> envs) {
        this.airlift_yarn = createAirliftYarn(envs);
    }

    protected ListenablePromise<AirliftYarn> createAirliftYarn(Map<String, String> envs) {
        return Promises.submit(() -> {
            return new AirliftYarn(envs);
        });
    }


    public ListenablePromise<Container> launchCoordinator() {
        return launcher().transformAsync((launcher) -> {
            return genPrestoContaienrConfig(true).transformAsync((config) -> {
                return launcher.launchContainer(config, Resource.newInstance(128, 1));
            });
        });
    }

    protected ListenablePromise<ContainerLauncher> launcher() {
        return airlift_yarn.transformAsync((yarn) -> yarn.launcher());
    }

    protected ListenablePromise<ContainerConfiguration> genPrestoContaienrConfig(boolean coordinator) {
        PrestoContainerConfig presto = new PrestoContainerConfig();
        presto.setCoordinator(coordinator);

        ContainerConfiguration configuration = new ContainerConfiguration(PrestoContainer.class);
        configuration.addAirliftStyleConfig(presto);

        return airliftConfig().transform((config) -> configuration.addAirliftStyleConfig(config));
    }

    protected ListenablePromise<AirliftConfig> airliftConfig() {
        return airlift_yarn.transformAsync((yarn) -> yarn.getAirlift()).transform((arilift) -> arilift.config());
    }


    public static void main(String args[]) throws Throwable {
        AirliftPresto presto = new AirliftPresto(System.getenv());
        presto.launchCoordinator().logException();

        LockSupport.park();
    }

    protected static ListenablePromise<Container> launcherCoordinator(ContainerLauncher launcher, ContainerConfiguration container_config, AirliftConfig airlif_config) {
        ContainerConfiguration configuration = new ContainerConfiguration(PrestoContainer.class);
        configuration.addAirliftStyleConfig(airlif_config);

        PrestoContainerConfig presto_container_config = new PrestoContainerConfig();
        presto_container_config.setCoordinator(true);
        configuration.addAirliftStyleConfig(presto_container_config);

        // deliver more config
        configuration.addAirliftStyleConfig(container_config.distill(YarnApplicationConfig.class));
        configuration.addAirliftStyleConfig(container_config.distill(UnionDiscoveryConfig.class));
        configuration.addAirliftStyleConfig(container_config.distill(HiveServicesConfig.class));

        return launcher.launchContainer(configuration, //
                Resource.newInstance(512, 1) //
        );
    }
}

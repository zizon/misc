package com.sf.misc.presto;

import com.google.gson.Gson;
import com.sf.misc.airlift.Airlift;
import com.sf.misc.airlift.AirliftConfig;
import com.sf.misc.async.ListenablePromise;
import com.sf.misc.async.Promises;
import com.sf.misc.presto.plugins.hive.HiveServicesConfig;
import com.sf.misc.yarn.AirliftYarnApplicationMaster;
import com.sf.misc.yarn.ContainerConfiguration;
import com.sf.misc.yarn.launcher.ContainerLauncher;
import io.airlift.log.Logger;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.File;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

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


    public ListenablePromise<Container> launchCoordinator(int memory) {
        return launchPrestoNode(memory, true);
    }

    public ListenablePromise<Container> launchWorker(int memory) {
        return launchPrestoNode(memory, false);
    }

    protected ListenablePromise<AirliftYarnApplicationMaster> createAirliftYarnApplicationMaster(Map<String, String> envs) {
        return Promises.submit(() -> {
            return new AirliftYarnApplicationMaster(envs);
        });
    }

    protected ListenablePromise<Container> launchPrestoNode(int memory, boolean coordinator) {
        LOGGER.info("launcher presto node:" + memory + " coordinator:" + coordinator);
        return launcher().transformAsync((launcher) -> {
            PrestoContainerConfig config = new PrestoContainerConfig();
            config.setCoordinator(coordinator);
            config.setMemory(memory);

            return genPrestoContaienrConfig(config).transformAsync((container_config) -> {
                return launcher.launchContainer(container_config);
            });
        });
    }

    protected ListenablePromise<ContainerLauncher> launcher() {
        return airlift_yarn_master.transformAsync(AirliftYarnApplicationMaster::launcher);
    }


    protected ListenablePromise<AirliftConfig> airliftConfig() {
        return airlift_yarn_master.transformAsync(AirliftYarnApplicationMaster::getAirlift) //
                .transformAsync(Airlift::effectiveConfig);
    }

    protected ListenablePromise<ContainerConfiguration> containerConfig() {
        return airlift_yarn_master.transformAsync(AirliftYarnApplicationMaster::containerConfiguration);
    }

    protected ListenablePromise<ContainerConfiguration> genPrestoContaienrConfig(PrestoContainerConfig presto_config) {
        return airliftConfig().transformAsync((airlift_config) -> {
            return containerConfig().transform((container_config) -> {
                // prepare container config
                ContainerConfiguration configuration = new ContainerConfiguration( //
                        PrestoContainer.class, //
                        container_config.group(), //
                        1, //
                        presto_config.getMemory(), //
                        container_config.classloader(), //
                        container_config.logLevels() //
                );

                // prepare presto config
                configuration.addAirliftStyleConfig(presto_config);

                // prepare airlift
                AirliftConfig presto_airlift_config = inherentConfig(airlift_config);
                configuration.addAirliftStyleConfig(presto_airlift_config);

                // parepare hive service config
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

        // foriegn discovery
        config.setFederationURI(parent_config.getFederationURI());

        // workers needs discovery uri
        config.setDiscovery(parent_config.getDiscovery());

        return config;
    }

    public static void main(String args[]) throws Throwable {
        LOGGER.info("start ailift presto master...");
        AirliftPresto presto = new AirliftPresto(System.getenv());

        // start coordinator
        Stream.of(
                Stream.of(presto.launchCoordinator(512)),
                IntStream.range(0, 1).parallel().mapToObj((ignore) -> {
                    return presto.launchWorker(512);
                })
        ).parallel() //
                .flatMap(Function.identity()) //
                .forEach(ListenablePromise::logException);

        ContainerId container_id = ConverterUtils.toContainerId(System.getenv().get(ApplicationConstants.Environment.CONTAINER_ID.key()));
        if (container_id.getApplicationAttemptId().getAttemptId() > 1) {
            LOGGER.info("wait presto master to die...");
            LockSupport.park();
        } else {
            LockSupport.parkNanos(TimeUnit.MINUTES.toNanos(2));
            System.exit(-1);
        }
    }
}

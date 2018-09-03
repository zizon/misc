package com.sf.misc.presto;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.sf.misc.airlift.Airlift;
import com.sf.misc.airlift.AirliftConfig;
import com.sf.misc.async.ListenablePromise;
import com.sf.misc.async.Promises;
import com.sf.misc.presto.modules.ClusterObserverModule;
import com.sf.misc.presto.modules.NodeRoleModule;
import com.sf.misc.presto.plugins.hive.HiveServicesConfig;
import com.sf.misc.yarn.AirliftYarnApplicationMaster;
import com.sf.misc.yarn.ContainerConfiguration;
import com.sf.misc.yarn.launcher.ContainerLauncher;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.log.Logger;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
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

    public ListenablePromise<Container> launchCoordinator(int memory, int cpu) {
        return clusterConfig().transformAsync((cluster_config) -> {
            return launchPrestoNode(cluster_config.getCoordinatorMemroy(), cpu, true);
        });
    }

    public ListenablePromise<Container> launchWorker(int memory, int cpu) {
        return clusterConfig().transformAsync((cluster_config) -> {
            return launchPrestoNode(cluster_config.getWorkerMemory(), cpu, false);
        });
    }

    protected ListenablePromise<AirliftYarnApplicationMaster> createAirliftYarnApplicationMaster(Map<String, String> envs) {
        return Promises.submit(() -> {
            return new AirliftYarnApplicationMaster(envs) {
                protected Collection<Module> modules() {
                    return AirliftPresto.this.modules();
                }
            };
        });
    }

    protected ListenablePromise<Container> launchPrestoNode(int memory, int cpu, boolean coordinator) {
        LOGGER.info("launcher presto node:" + memory + " coordinator:" + coordinator);
        return launcher().transformAsync((launcher) -> {
            PrestoContainerConfig config = new PrestoContainerConfig();
            config.setCoordinator(coordinator);
            config.setMemory(memory);
            config.setCpu(cpu);

            return genPrestoContaienrConfig(config).transformAsync((container_config) -> {
                return launcher.launchContainer(container_config);
            });
        });
    }

    public ListenablePromise<ContainerLauncher> launcher() {
        return airlift_yarn_master.transformAsync(AirliftYarnApplicationMaster::launcher);
    }

    protected ListenablePromise<AirliftConfig> airliftConfig() {
        return airlift_yarn_master.transformAsync(AirliftYarnApplicationMaster::getAirlift) //
                .transformAsync(Airlift::effectiveConfig);
    }

    public ListenablePromise<ContainerConfiguration> containerConfig() {
        return airlift_yarn_master.transformAsync(AirliftYarnApplicationMaster::containerConfiguration);
    }

    protected ListenablePromise<ContainerConfiguration> genPrestoContaienrConfig(PrestoContainerConfig presto_config) {
        return airliftConfig().transformAsync((airlift_config) -> {
            return containerConfig().transform((container_config) -> {
                // prepare container config
                ContainerConfiguration configuration = new ContainerConfiguration( //
                        PrestoContainer.class, //
                        container_config.group(), //
                        presto_config.getCpu(), //
                        presto_config.getMemory(), //
                        container_config.classloader(), //
                        container_config.logLevels() //
                );

                // prepare presto config
                configuration.addContextConfig(presto_config);

                // prepare airlift
                AirliftConfig presto_airlift_config = inherentConfig(airlift_config);
                configuration.addContextConfig(presto_airlift_config);

                // parepare hive service config
                configuration.addContextConfig(container_config.distill(HiveServicesConfig.class));

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

        return config;
    }

    protected ListenablePromise<PrestoClusterConfig> clusterConfig() {
        return containerConfig() //
                .transform((container_config) -> container_config.distill(PrestoClusterConfig.class));
    }

    protected Collection<Module> modules() {
        return ImmutableList.<Module>builder() //
                .add(new ClusterObserverModule(
                                this,
                                ConverterUtils.toContainerId //
                                        (System.getenv().get(ApplicationConstants.Environment.CONTAINER_ID.key())) //
                        ) //
                ) //
                .add(new NodeRoleModule(ConverterUtils.toContainerId //
                                (System.getenv().get(ApplicationConstants.Environment.CONTAINER_ID.key())),
                                NodeRoleModule.ContainerRole.ApplicationMaster
                        )
                ) //
                .build();
    }

    protected static void testFailOver() {
        ContainerId container_id = ConverterUtils.toContainerId(System.getenv().get(ApplicationConstants.Environment.CONTAINER_ID.key()));

        if (container_id.getApplicationAttemptId().getAttemptId() > 1) {
            LOGGER.info("wait presto master to die...");
            LockSupport.park();
        } else {
            LockSupport.parkNanos(TimeUnit.MINUTES.toNanos(2));
            System.exit(-1);
        }
    }

    public static void main(String args[]) throws Throwable {
        LOGGER.info("start ailift presto master...");
        new AirliftPresto(System.getenv());

        LockSupport.park();
    }
}

package com.sf.misc.presto.modules;

import com.google.common.collect.Comparators;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.sf.misc.airlift.federation.ServiceSelectors;
import com.sf.misc.async.Entrys;
import com.sf.misc.async.ListenablePromise;
import com.sf.misc.async.Promises;
import com.sf.misc.presto.AirliftPresto;
import com.sf.misc.presto.PrestoClusterConfig;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.log.Logger;
import io.airlift.node.NodeModule;
import kafka.security.auth.Deny;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.ContainerState;

import javax.annotation.PostConstruct;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class ClusterObserver {

    public static final Logger LOGGER = Logger.get(ClusterObserver.class);

    protected static final String COORDINATOR = "presto-coordinator";
    protected static final String WORKER = "presto";

    protected final AirliftPresto presto;
    protected final ListenablePromise<PrestoClusterConfig> cluster_config;
    protected final ServiceSelectors selectors;
    protected final ContainerId container_id;

    @Inject
    public ClusterObserver(AirliftPresto presto,
                           ServiceSelectors selectors,
                           ContainerId container_id
    ) {
        this.presto = presto;
        this.selectors = selectors;
        this.container_id = container_id;
        this.cluster_config = presto.containerConfig() //
                .transform((config) -> config.distill(PrestoClusterConfig.class));
    }

    @PostConstruct
    public void start() {
        Promises.schedule( //
                this::scheudle, //
                TimeUnit.SECONDS.toMillis(5), //
                true //
        ).logException();
    }

    protected void scheudle() {
        // find running containers
        ListenablePromise<ConcurrentMap<ContainerId, NodeRoleModule.ContainerRole>> running_containers = findClusterContainers().callback((containers) -> {
            if (LOGGER.isDebugEnabled()) {
                String detail = containers.entrySet().parallelStream()
                        .map((entry) -> {
                            return "container:" + entry.getKey() + " role:" + entry.getValue();
                        }).collect(Collectors.joining("\n"));
                LOGGER.debug("live container detail:\n" + detail);
            }
        });

        // see if contianer startup?
        ListenablePromise<Boolean> discovery_ready = running_containers.transform((containers) -> {
            boolean unknow_role_container = containers.entrySet().parallelStream()
                    // filter this application master
                    .filter((entry) -> entry.getValue() == NodeRoleModule.ContainerRole.Unknown)
                    .findAny()
                    .isPresent();
            // all containers has its role
            return !unknow_role_container;
        });

        // ensure cluster ready
        discovery_ready.callback((ready) -> {
            if (!ready) {
                LOGGER.info("container master register not ready,backoff...");
                return;
            }

            // group contaienr by role
            Map<NodeRoleModule.ContainerRole, ListenablePromise<Set<ContainerId>>> group_container = Stream.of(
                    NodeRoleModule.ContainerRole.Coordinator,
                    NodeRoleModule.ContainerRole.Worker
            ).parallel().map((role) -> {
                return Entrys.newImmutableEntry(role, running_containers.transform((containers) -> {
                            return containers.entrySet().parallelStream()
                                    // filter by role
                                    .filter((entry) -> entry.getValue() == role)
                                    // retrive container id
                                    .map(Map.Entry::getKey)
                                    .collect(Collectors.toSet());
                        })
                );
            }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            // scale coordinator
            cluster_config.callback((config) -> {
                // scale coordinator
                group_container.get(NodeRoleModule.ContainerRole.Coordinator).callback((continaers) -> {
                    LOGGER.debug("monitor coordinator scale...");
                    scaleCoordinator(config, continaers);
                });

                // scale worker
                group_container.get(NodeRoleModule.ContainerRole.Worker).callback((continaers) -> {
                    LOGGER.debug("monitor worker scale...");
                    scaleWorker(config, continaers);
                });
            });
        });
    }

    protected ListenablePromise<ConcurrentMap<ContainerId, NodeRoleModule.ContainerRole>> findClusterContainers() {
        ListenablePromise<Set<ContainerId>> running_containers = selectors.selectServiceForType(NodeRoleModule.SERVICE_TYPE).parallelStream()
                .map(NodeRoleModule::containerId)
                .map(ContainerId::getApplicationAttemptId)
                .distinct()
                .map((applicateion_attempt) -> {
                    return presto.launcher().transformAsync((launcher) -> {
                        return launcher.listContainer(applicateion_attempt);
                    });
                })
                .reduce(Promises.reduceCollectionsOperator())
                .orElse(Promises.immediate(Collections.emptyList()))
                .transform((reports) -> {
                    // collect alive only
                    return reports.parallelStream()
                            .filter((report) -> report.getContainerState() != ContainerState.COMPLETE)
                            .map((report) -> report.getContainerId())
                            .collect(Collectors.toSet());
                });

        // find known container roles
        ConcurrentMap<ContainerId, NodeRoleModule.ContainerRole> contaienr_roles = //
                selectors.selectServiceForType(NodeRoleModule.SERVICE_TYPE).parallelStream()
                        .map((service) -> {
                            LOGGER.debug("touch presto node:" + service);
                            return Entrys.newImmutableEntry(
                                    NodeRoleModule.containerId(service),
                                    NodeRoleModule.role(service)
                            );
                        }) // filter coordinator and worker
                        .collect(Collectors.toConcurrentMap(Map.Entry::getKey, Map.Entry::getValue));

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("contaienr roles:\n" +
                    contaienr_roles.entrySet().parallelStream()
                            .map((entry -> "contaienr_id:" + entry.getKey() + " role:" + entry.getValue()))
                            .collect(Collectors.joining("\n"))
            );
        }

        // compare yarn report and airlift report,
        // annote this group containers with container role.
        return running_containers.transform((containers) -> {
            return containers.parallelStream()
                    .map((container) -> {
                        LOGGER.debug("annotating container:" + container);
                        return Entrys.newImmutableEntry( //
                                container, //
                                contaienr_roles.getOrDefault( //
                                        container, //
                                        NodeRoleModule.ContainerRole.Unknown //
                                ) //
                        );
                    })
                    .collect(Collectors.toConcurrentMap(Map.Entry::getKey, Map.Entry::getValue));
        });
    }

    protected void scaleCoordinator(PrestoClusterConfig config, Set<ContainerId> containers) {
        LOGGER.debug("config coordinator:" + 1 + " live:" + containers.size());

        if (containers.isEmpty()) {
            LOGGER.debug("launcher one coordinator");
            presto.launchCoordinator(config.getCoordinatorMemroy(), config.getCoordinatorCpu()).logException();
            return;
        }

        // keep only one coordinator
        // use deterministic order
        Iterators.limit(containers.parallelStream().sorted().iterator(), containers.size() - 1)
                .forEachRemaining((extra_container) -> {
                    if (!extra_container.getApplicationAttemptId().equals(container_id.getApplicationAttemptId())) {
                        LOGGER.debug("this application " + container_id.getApplicationAttemptId() //
                                + " is not owner of coordinatro:" + extra_container + " ,skip releasing");
                        return;
                    }

                    LOGGER.info("release extrac coordiantor:" + extra_container);
                    presto.launcher() //
                            .transform((launcher) -> launcher.releaseContainer(extra_container)) //
                            .logException();
                });
        return;
    }

    protected void scaleWorker(PrestoClusterConfig config, Set<ContainerId> containers) {
        LOGGER.debug("config worker:" + config.getNumOfWorkers() + " live:" + containers.size());

        if (config.getNumOfWorkers() == containers.size()) {
            LOGGER.debug("worker number aggree, config:" + config.getNumOfWorkers() + " container:" + containers.size());
            return;
        } else if (containers.size() < config.getNumOfWorkers()) {
            LOGGER.debug("try launch worker:" + (config.getNumOfWorkers() - containers.size()));
            // allocate more
            IntStream.range(0, config.getNumOfWorkers() - containers.size()).parallel()
                    .forEach((ignore) -> {
                        LOGGER.debug("launcher one woker...");
                        presto.launchWorker(config.getWorkerMemory(), config.getWorkerCpu()).logException();
                    });
            return;
        } else {
            // select some to release
            // use deterministic order
            Iterators.limit(containers.parallelStream().sorted().iterator(), containers.size() - config.getNumOfWorkers())
                    .forEachRemaining((extra_container) -> {
                        if (!extra_container.getApplicationAttemptId().equals(container_id.getApplicationAttemptId())) {
                            LOGGER.debug("this application " + container_id.getApplicationAttemptId() //
                                    + " is not owner of worker:" + extra_container + " ,skip releasing");
                            return;
                        }

                        LOGGER.info("release extra worker:" + extra_container);
                        presto.launcher() //
                                .transform((launcher) -> launcher.releaseContainer(extra_container)) //
                                .logException();
                    });
            return;
        }
    }
}

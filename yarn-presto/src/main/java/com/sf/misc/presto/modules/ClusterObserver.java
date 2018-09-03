package com.sf.misc.presto.modules;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.sf.misc.airlift.federation.ServiceSelectors;
import com.sf.misc.async.Entrys;
import com.sf.misc.async.ListenablePromise;
import com.sf.misc.async.Promises;
import com.sf.misc.presto.AirliftPresto;
import com.sf.misc.presto.PrestoClusterConfig;
import io.airlift.log.Logger;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.checkerframework.checker.units.qual.K;

import javax.annotation.PostConstruct;
import java.util.Collections;
import java.util.Comparator;
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
    protected final LoadingCache<ContainerId, NodeRoleModule.ContainerRole> contaienr_roles;

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

        this.contaienr_roles = CacheBuilder.newBuilder()
                .expireAfterAccess(5, TimeUnit.MINUTES)
                .refreshAfterWrite(5, TimeUnit.SECONDS)
                .build(new CacheLoader<ContainerId, NodeRoleModule.ContainerRole>() {

                    @Override
                    public ListenableFuture<NodeRoleModule.ContainerRole> reload(ContainerId container_id, NodeRoleModule.ContainerRole old) throws Exception {
                        if (old != NodeRoleModule.ContainerRole.Unknown) {
                            // once node is know, it will not change
                            return Promises.immediate(old);
                        }

                        // search in selector
                        return Promises.submit(() -> load(container_id)) //
                                .logException((ignore) -> "fail to load contaienr role:" + container_id);
                    }

                    @Override
                    public NodeRoleModule.ContainerRole load(ContainerId container_id) throws Exception {
                        return selectors.selectServiceForType(NodeRoleModule.SERVICE_TYPE).parallelStream()
                                .filter((service) -> NodeRoleModule.containerId(service).equals(container_id))
                                .map((service) -> Entrys.newImmutableEntry( //
                                        NodeRoleModule.containerId(service), //
                                        NodeRoleModule.role(service) //
                                        ) //
                                ) //
                                .collect(Collectors.toConcurrentMap(Map.Entry::getKey, Map.Entry::getValue))
                                .getOrDefault(container_id, NodeRoleModule.ContainerRole.Unknown);
                    }
                });
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
        return selectors.selectServiceForType(NodeRoleModule.SERVICE_TYPE).parallelStream()
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
                            // find not dead
                            .filter((report) -> report.getContainerState() != ContainerState.COMPLETE)
                            // lookup from auto refresh cache
                            .map((report) -> Entrys.newImmutableEntry(report.getContainerId(), contaienr_roles.getUnchecked(report.getContainerId())))
                            .collect(Collectors.toConcurrentMap(Map.Entry::getKey, Map.Entry::getValue));
                }) //
                .callback((containers) -> {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("contaienr roles:\n" +
                                containers.entrySet().parallelStream()
                                        .map((entry -> "contaienr_id:" + entry.getKey() + " role:" + entry.getValue()))
                                        .collect(Collectors.joining("\n"))
                        );
                    }
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

package com.sf.misc.yarn;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.sf.misc.async.ExecutorServices;
import com.sf.misc.presto.PrestoContainerLauncher;
import io.airlift.concurrent.MoreFutures;
import io.airlift.log.Logger;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ContainerAssurance {

    public static final Logger LOGGER = Logger.get(ContainerAssurance.class);

    protected PrestoContainerLauncher launcher;
    protected YarnApplication application;
    protected ConcurrentMap<String, Set<Container>> container_group;

    @Inject
    public ContainerAssurance(PrestoContainerLauncher launcher, YarnApplication application) {
        this.launcher = launcher;
        this.application = application;

        this.container_group = Maps.newConcurrentMap();
    }

    public ListenableFuture<Boolean> secure(String group_name, Supplier<ListenableFuture<Container>> contaienr_supplier, int nodes) {
        Set<Container> containers = this.container_group.compute(group_name, (key, old) -> {
            if (old == null) {
                old = Sets.newConcurrentHashSet();
            }
            return old;
        });

        // fresh node status
        ListenableFuture<Integer> life = containers.parallelStream() //
                .map((container) -> {
                    ListenableFuture<ContainerStatus> status_future = launcher.launcher().containerStatus(container);

                    //LOGGER.info("fetching container status:" + container);

                    // find live node ,and remove dead one
                    return Futures.catching(
                            Futures.transform(status_future, (status) -> {
                                if (status.getState() == ContainerState.COMPLETE) {
                                    LOGGER.info("remove dead container:" + container);
                                    containers.remove(container);
                                    return 0;
                                }
                                return 1;
                            }, ExecutorServices.executor()), //
                            Throwable.class, //
                            (exception) -> {
                                return 0;
                            },
                            ExecutorServices.executor()
                    );
                }) //
                .reduce((left, right) -> {
                    return Futures.transformAsync(left, (left_life) -> {
                        return Futures.transform(right, (right_life) -> {
                            return left_life + right_life;
                        }, ExecutorServices.executor());
                    }, ExecutorServices.executor());
                }).orElse(Futures.immediateFuture(0));

        // pregnant new life
        ListenableFuture<List<ListenableFuture<Container>>> zygotes = Futures.transform(life, (live) -> {
            int pregnant = nodes - live;
            if (pregnant == 0) {
                LOGGER.info("no live to spwan");
                return ImmutableList.of();
            } else if (pregnant < 0) {
                // should kill some?
                LOGGER.warn("group:" + group_name + " has more contaienr:" + live + " than requested:" + nodes);
                return ImmutableList.of();
            }

            LOGGER.info("live:" + live + " kick:" + pregnant + " require:" + nodes);
            return IntStream.range(0, pregnant).parallel().mapToObj((ignore) -> {
                LOGGER.info("kick container supplier");
                return contaienr_supplier.get();
            }).collect(Collectors.toList());
        }, ExecutorServices.executor());

        // look after babys
        ListenableFuture<Boolean> baby_sit = Futures.transformAsync(zygotes, (zynote) -> {
            return zynote.parallelStream() //
                    .map((container_future) -> {
                        return Futures.catching( //
                                Futures.transform(container_future, (container) -> {
                                    if (!containers.add(container)) {
                                        LOGGER.warn("group:" + group_name + " fail to register container:" + container);
                                        return false;
                                    }

                                    LOGGER.info("secure one");
                                    return true;
                                }, ExecutorServices.executor()), //
                                Throwable.class, //
                                (exception) -> {
                                    LOGGER.error(exception, "fail to allocate one container for group:" + group_name);
                                    return false;
                                }, //
                                ExecutorServices.executor()
                        );
                    }) //
                    .reduce((left, right) -> {
                        return Futures.transformAsync(left, (left_ok) -> {
                            return Futures.transform(right, (right_ok) -> {
                                return left_ok && right_ok;
                            },ExecutorServices.executor());
                        },ExecutorServices.executor());
                    }).orElse(Futures.immediateFuture(true));
        }, ExecutorServices.executor());

        return baby_sit;
    }

    public ImmutableSet<Container> containers(String group_name) {
        Set<Container> containers = this.container_group.get(group_name);
        if (containers == null) {
            return ImmutableSet.of();
        }

        return ImmutableSet.<Container>builder().addAll(containers).build();
    }
}

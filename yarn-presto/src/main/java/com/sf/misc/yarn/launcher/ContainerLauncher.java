package com.sf.misc.yarn.launcher;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.sf.misc.async.ListenablePromise;
import com.sf.misc.async.Promises;
import com.sf.misc.async.SettablePromise;
import com.sf.misc.yarn.ContainerConfiguration;
import com.sf.misc.yarn.rpc.YarnNMProtocol;
import com.sf.misc.yarn.rpc.YarnRMProtocol;
import io.airlift.log.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.security.NMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class ContainerLauncher {
    public static final Logger LOGGER = Logger.get(ContainerLauncher.class);

    public static enum Enviroments {
        CONTAINER_LOG_DIR;
    }

    protected final ListenablePromise<YarnRMProtocol> master_service;
    protected final ListenablePromise<LauncherEnviroment> enviroment;

    protected final AtomicInteger request_sequence;
    protected final ConcurrentMap<NodeId, ListenablePromise<NMToken>> node_tokens;
    protected final LoadingCache<Container, ListenablePromise<YarnNMProtocol>> container_services;
    protected final ConcurrentMap<Resource, Queue<SettablePromise<Container>>> container_asking;
    protected final Queue<Resource> resource_asking;

    public ContainerLauncher(
            ListenablePromise<YarnRMProtocol> master_service,
            ListenablePromise<LauncherEnviroment> enviroment,
            boolean nohearbeat
    ) {
        this.master_service = master_service;
        this.enviroment = enviroment;
        this.container_services = setupContainerServiceCache();
        this.request_sequence = new AtomicInteger(0);
        this.node_tokens = Maps.newConcurrentMap();

        this.container_asking = Maps.newConcurrentMap();
        this.resource_asking = Queues.newConcurrentLinkedQueue();

        if (!nohearbeat) {
            startHeartbeats();
        }
    }

    public Resource jvmOverhead(Resource resource) {
        return Resource.newInstance((int) (resource.getMemory() * 1.2), resource.getVirtualCores());
    }

    public ListenablePromise<ContainerId> releaseContainer(Container container) {
        return this.container_services.getUnchecked(container).transformAsync((container_service) -> {
            StopContainersResponse response = container_service.stopContainers( //
                    StopContainersRequest.newInstance(ImmutableList.of(container.getId())) //
            );

            if (response.getSuccessfullyStoppedContainers().size() == 1) {
                return Promises.immediate(response.getSuccessfullyStoppedContainers().get(0));
            }

            return Promises.failure(response.getFailedRequests().get(container.getId()).deSerialize());
        });
    }

    public ListenablePromise<ContainerStatus> containerStatus(Container container) {
        return this.container_services.getUnchecked(container).transformAsync((rpc) -> {
            GetContainerStatusesResponse response = rpc.getContainerStatuses(//
                    GetContainerStatusesRequest.newInstance(ImmutableList.of(container.getId()))
            );
            if (response.getContainerStatuses().size() == 1) {
                return Promises.immediate(response.getContainerStatuses().get(0));
            }

            return Promises.failure(response.getFailedRequests().get(container.getId()).deSerialize());
        });
    }

    public ListenablePromise<ContainerLaunchContext> createContext(ContainerConfiguration container_config) throws MalformedURLException {
        return enviroment.transformAsync((launcher) -> {
            return launcher.launcherCommand(
                    Resource.newInstance(container_config.getCpu(), container_config.getMemory()), //
                    Class.forName(container_config.getMaster()) //
            ).transform((commands) -> {
                Map<String, String> combinde_enviroments = Maps.newHashMap();

                // container config
                combinde_enviroments.put(ContainerConfiguration.class.getName(), ContainerConfiguration.encode(container_config));

                // this launcher enviroment
                combinde_enviroments.putAll(launcher.enviroments());

                // create
                return ContainerLaunchContext.newInstance( //
                        Collections.emptyMap(), //
                        combinde_enviroments, //
                        commands, //
                        null, //
                        null, //
                        null //
                );
            });
        });
    }

    public ListenablePromise<Container> launchContainer(ContainerConfiguration container_config) {
        // allocated notifier
        SettablePromise<Container> allocated_container = SettablePromise.create();

        // when allocate completed
        ListenablePromise<Container> started_container = allocated_container.transformAsync((allocated) -> {
            return container_services.get(allocated).transformAsync((container_service) -> {
                return createContext(container_config) //
                        .transformAsync((context) -> {
                            // send start reqeust
                            StartContainersResponse response = container_service.startContainers(
                                    StartContainersRequest.newInstance( //
                                            Collections.singletonList( //
                                                    StartContainerRequest.newInstance(
                                                            context, //
                                                            allocated.getContainerToken() //
                                                    )) //
                                    ) //
                            );

                            LOGGER.info("start container..." + allocated + " on node:" + container_service + " response:" + response.getSuccessfullyStartedContainers().size());
                            if (response.getSuccessfullyStartedContainers().size() == 1) {
                                return Promises.immediate(allocated);
                            }

                            return Promises.failure(response.getFailedRequests().get(allocated.getId()).deSerialize());
                        });
            });
        });

        // add overhead
        Resource resource = jvmOverhead( //
                Resource.newInstance( //
                        container_config.getCpu(), //
                        container_config.getMemory() //
                ) //
        );

        // ask container
        container_asking.compute(
                resource, //
                (key, old) -> {
                    if (old == null) {
                        old = Queues.newConcurrentLinkedQueue();
                    }

                    old.offer(allocated_container);
                    return old;
                } //
        );

        // ask resource
        resource_asking.offer(resource);

        return started_container;
    }

    protected void startHeartbeats() {
        master_service.callback((master_service, throwable) -> {
            if (throwable != null) {
                LOGGER.warn(throwable, "master initialize fail?");
            }

            Promises.schedule( //
                    () -> {
                        // start heart beart
                        AllocateResponse response = master_service.allocate(AllocateRequest.newInstance(
                                request_sequence.incrementAndGet(),
                                Float.NaN,
                                drainResrouceRequest(),
                                Collections.emptyList(),
                                ResourceBlacklistRequest.newInstance(
                                        Collections.emptyList(), //
                                        Collections.emptyList() //
                                ))
                        );

                        masterHeartbeat(response);
                    }, //
                    TimeUnit.SECONDS.toMillis(5) //
            );
        });
    }

    protected List<ResourceRequest> drainResrouceRequest() {
        return Lists.newArrayList(
                Iterators.consumingIterator(resource_asking.iterator())
        ).parallelStream()
                .collect(
                        Maps::<Resource, ResourceRequest>newConcurrentMap,
                        (map, resource) -> {
                            map.compute(resource, (key, value) -> {
                                // accumulate same resource reqeust
                                if (value == null) {
                                    value = ResourceRequest.newInstance(
                                            Priority.UNDEFINED,
                                            ResourceRequest.ANY,
                                            key,
                                            1
                                    );
                                } else {
                                    value = ResourceRequest.newInstance(
                                            Priority.UNDEFINED,
                                            ResourceRequest.ANY,
                                            key,
                                            value.getNumContainers() + 1
                                    );
                                }
                                return value;
                            });
                        },
                        (left, right) -> {
                            // merge left and right
                            right.entrySet().parallelStream() //
                                    .forEach((entry) -> {
                                        left.compute(entry.getKey(), (key, value) -> {
                                            if (value != null) {
                                                value = ResourceRequest.newInstance(
                                                        Priority.UNDEFINED,
                                                        ResourceRequest.ANY,
                                                        key,
                                                        entry.getValue().getNumContainers() + 1
                                                );
                                            }

                                            return value;
                                        });
                                    });
                        }
                ) //
                .values().parallelStream() //
                .collect(Collectors.toList());
    }

    protected LoadingCache<Container, ListenablePromise<YarnNMProtocol>> setupContainerServiceCache() {
        ListenablePromise<YarnNMProtocol> container_service_factory = master_service.transformAsync((yarn_rm_protocol) -> {
            // prepare config
            Configuration configuration = new Configuration();
            new ConfigurationGenerator().generateYarnConfiguration(yarn_rm_protocol.config().getRMs()).entrySet() //
                    .forEach((entry) -> {
                                configuration.set(entry.getKey(), entry.getValue());
                            } //
                    );

            // build nodemanager protocl factory
            return YarnNMProtocol.create(yarn_rm_protocol.ugi(), configuration);
        });

        return CacheBuilder.newBuilder() //
                .expireAfterAccess(5, TimeUnit.MINUTES) //
                .removalListener((RemovalNotification<Container, ListenablePromise<YarnNMProtocol>> notice) -> {
                    // auto close after 5 miniute
                    notice.getValue().callback((container_service, throwable) -> {
                        // auto close in 5 minute
                        try (YarnNMProtocol ignore = container_service) {
                            if (throwable != null) {
                                LOGGER.error(throwable, "fail to stop container:" + notice.getKey());
                                return;
                            }
                        }
                    });
                }).build(new CacheLoader<Container, ListenablePromise<YarnNMProtocol>>() {
                    @Override
                    public ListenablePromise<YarnNMProtocol> load(Container key) throws Exception {
                        return container_service_factory.transformAsync((factory) -> {
                            return factory.connect(key.getNodeId().getHost(), key.getNodeId().getPort());
                        });
                    }
                });
    }

    protected void masterHeartbeat(AllocateResponse response) {
        Queue<org.apache.hadoop.security.token.Token> tokens = Queues.newConcurrentLinkedQueue();

        if (response.getAMRMToken() != null) {
            Token token = response.getAMRMToken();

            LOGGER.info("update application master token..." + token);
            tokens.offer(ConverterUtils.convertFromYarn(token, new Text(token.getService())));
        }

        // update node token
        if (response.getNMTokens() != null) {
            response.getNMTokens().parallelStream().forEach((token) -> {
                LOGGER.info("update node manager token..." + token);
                org.apache.hadoop.security.token.Token<NMTokenIdentifier> node_manager_token =
                        ConverterUtils.convertFromYarn(token.getToken(), new InetSocketAddress(token.getNodeId().getHost(), token.getNodeId().getPort()));

                tokens.offer(node_manager_token);
            });
        }

        // assigned
        master_service.callback((master_service, throwable) -> {
            tokens.stream().forEach(master_service.ugi()::addToken);
            assign(response.getAllocatedContainers());
        });
    }

    protected void assign(List<Container> containers) {
        if (containers == null || containers.isEmpty()) {
            return;
        }

        Promises.submit(() -> doAssign(containers)).callback((remainds, throwable) -> {
            if (throwable != null) {
                LOGGER.error(throwable, "fail when assining container:" + containers);
                return;
            }

            assign(remainds);
        });
    }

    protected List<Container> doAssign(List<Container> usable_container) {
        Iterator<Map.Entry<Resource, Queue<SettablePromise<Container>>>> asking = container_asking.entrySet()
                .parallelStream().sorted().iterator();

        // sort by resource
        Iterator<Container> containers = usable_container.parallelStream().sorted((left, right) -> {
            return left.getResource().compareTo(right.getResource());
        }).iterator();

        // keep unused container
        List<Container> not_assigned = Lists.newLinkedList();

        // do assgin
        while (containers.hasNext()) {
            Container container = containers.next();
            Resource container_resoruce = container.getResource();
            SettablePromise<Container> assigned = null;

            while (asking.hasNext()) {
                // poll resoruce ask
                Map.Entry<Resource, Queue<SettablePromise<Container>>> entry = asking.next();

                // find assign ack responder
                Queue<SettablePromise<Container>> waiting = entry.getValue();
                Resource ask = entry.getKey();

                // skip empty
                if (waiting.isEmpty()) {
                    continue;
                }

                // found matched
                if (container_resoruce.compareTo(ask) >= 0) {
                    LOGGER.info("assign container:" + container + " to ask:" + ask);
                    assigned = waiting.poll();

                    // find one , stop
                    if (assigned != null) {
                        break;
                    }
                }
            }

            if (assigned != null) {
                // find suitable one,assign to it
                assigned.set(container);
            } else {
                // no situable, keep it
                not_assigned.add(container);
            }
        }

        // log context
        container_asking.entrySet().stream().filter((entry) -> entry.getValue().size() > 0).forEach((entry) -> {
            LOGGER.info("waiting list:" + entry.getKey() + " size:" + entry.getValue().size());
        });

        LOGGER.info("usable container:" + usable_container + " not assigned:" + not_assigned);
        return not_assigned;
    }
}

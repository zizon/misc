package com.sf.misc.yarn;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.sf.misc.async.ListenablePromise;
import com.sf.misc.async.Promises;
import com.sf.misc.async.SettablePromise;
import io.airlift.log.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.security.NMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;

import javax.security.auth.callback.Callback;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class ContainerLauncher {
    public static final Logger LOGGER = Logger.get(ContainerLauncher.class);

    public static enum Enviroments {
        CONTAINER_LOG_DIR;
    }

    protected final ListenablePromise<YarnRMProtocol> master;
    protected final ListenablePromise<LauncherEnviroment> launcher;
    protected final ListenablePromise<YarnNMProtocol> containers;

    protected final AtomicInteger response_id;
    protected final ConcurrentMap<NodeId, ListenablePromise<NMToken>> node_tokens;
    protected final LoadingCache<Container, ListenablePromise<YarnNMProtocol>> node_rpc_proxys;
    protected final ConcurrentMap<Resource, Queue<SettablePromise<Container>>> container_asking;
    protected final Queue<ResourceRequest> resources_asking;

    public ContainerLauncher(
            ListenablePromise<YarnRMProtocol> protocol,
            ListenablePromise<LauncherEnviroment> launcher,
            boolean nohearbeat
    ) {
        this.master = protocol;
        this.launcher = launcher;
        this.containers = master.transformAsync((instance) -> {
            Configuration configuration = new Configuration();
            new ConfigurationGenerator().generateYarnConfiguration(instance.config().getRMs()).entrySet() //
                    .forEach((entry) -> {
                                configuration.set(entry.getKey(), entry.getValue());
                            } //
                    );
            return YarnNMProtocol.create(instance.ugi(), configuration, instance.doAS(() -> YarnRPC.create(configuration)));
        });

        this.node_rpc_proxys = buildNodeRPCProxyCache();
        this.response_id = new AtomicInteger(0);
        this.node_tokens = Maps.newConcurrentMap();

        this.container_asking = Maps.newConcurrentMap();
        this.resources_asking = Queues.newConcurrentLinkedQueue();

        if (!nohearbeat) {
            startHeartbeats();
        }
    }

    public ListenablePromise<LauncherEnviroment> enviroments() {
        return this.launcher;
    }

    public ListenablePromise<Container> launchContainer(ContainerConfiguration container_config, Resource resource) {
        return this.launchContainer(container_config, resource, null, null);
    }

    public Resource jvmResource(Resource resource) {
        return Resource.newInstance((int) (resource.getMemory() * 1.2), resource.getVirtualCores());
    }

    public ListenablePromise<Container> launchContainer(ContainerConfiguration container_config, Resource
            resource, Map<String, String> properties, Map<String, String> enviroments) {
        // allocated notifier
        SettablePromise<Container> allocated_container = SettablePromise.create();

        // launched notifier
        ListenablePromise<Container> launched_container = allocated_container.transformAsync((allocated) -> {
            return node_rpc_proxys.get(allocated).transformAsync((node) -> {
                ImmutableMap<String, String> envs = ImmutableMap.<String, String>builder()
                        .putAll(Optional.ofNullable(enviroments).orElse(Collections.emptyMap()))
                        .put(ContainerConfiguration.class.getName(), ContainerConfiguration.embedded(container_config))
                        .build();

                return createContext( //
                        resource, //
                        container_config, //
                        Optional.ofNullable(properties).orElse(Collections.emptyMap()), //
                        envs//
                ).transformAsync((context) -> {
                    StartContainersResponse response = node.startContainers(
                            StartContainersRequest.newInstance( //
                                    Collections.singletonList( //
                                            StartContainerRequest.newInstance(
                                                    context, //
                                                    allocated.getContainerToken() //
                                            )) //
                            ) //
                    );

                    LOGGER.info("start container..." + allocated + " on node:" + node + " response:" + response.getSuccessfullyStartedContainers().size());
                    if (response.getSuccessfullyStartedContainers().size() == 1) {
                        return Promises.immediate(allocated);
                    }

                    return Promises.failure(response.getFailedRequests().get(allocated.getId()).deSerialize());
                });
            });
        });

        container_asking.compute(jvmResource(resource), (key, old) -> {
            if (old == null) {
                old = Queues.newConcurrentLinkedQueue();
            }

            old.offer(allocated_container);
            return old;
        });

        int id = response_id.incrementAndGet();
        LOGGER.info("request container:" + container_config.getMaster() + " resource:(" + resource + "/" + jvmResource(resource) + ") response_id:" + id);
        resources_asking.offer(ResourceRequest.newInstance( //
                Priority.UNDEFINED, //
                ResourceRequest.ANY, //
                resource, //
                1 //
        ));

        return launched_container;
    }

    public ListenablePromise<ContainerId> releaseContainer(Container container) {
        return this.node_rpc_proxys.getUnchecked(container).transformAsync((rpc) -> {
            StopContainersResponse response = rpc.stopContainers( //
                    StopContainersRequest.newInstance(ImmutableList.of(container.getId())) //
            );
            if (response.getSuccessfullyStoppedContainers().size() == 1) {
                return Promises.immediate(response.getSuccessfullyStoppedContainers().stream().findAny().get());
            }

            return Promises.failure(response.getFailedRequests().get(container.getId()).deSerialize());
        });
    }

    public ListenablePromise<ContainerStatus> containerStatus(Container container) {
        return this.node_rpc_proxys.getUnchecked(container).transformAsync((rpc) -> {
            GetContainerStatusesResponse response = rpc.getContainerStatuses(//
                    GetContainerStatusesRequest.newInstance(ImmutableList.of(container.getId()))
            );
            if (response.getContainerStatuses().size() == 1) {
                return Promises.immediate(response.getContainerStatuses().stream().findAny().get());
            }

            return Promises.failure(response.getFailedRequests().get(container.getId()).deSerialize());
        });
    }

    public ListenablePromise<ContainerLaunchContext> createContext(Resource resource, //
                                                                   ContainerConfiguration container_config, //
                                                                   Map<String, String> properties,  //
                                                                   Map<String, String> enviroment //
    ) throws MalformedURLException {
        return launcher.transformAsync((instance) -> {
            return instance.launcherCommand(
                    resource, //
                    properties, //
                    Class.forName(container_config.getMaster()) //
            ).transform((commands) -> {
                ImmutableMap<String, String> envs = ImmutableMap.<String, String>builder()
                        .put(ContainerConfiguration.class.getName(), ContainerConfiguration.embedded(container_config))
                        .putAll(instance.enviroments()) //
                        .putAll(Optional.ofNullable(enviroment).orElse(Collections.emptyMap())) //
                        .build();
                return ContainerLaunchContext.newInstance(Collections.emptyMap(), //
                        envs, //
                        commands, //
                        null, //
                        null, //
                        null);
            });
        });
    }

    protected void startHeartbeats() {
        master.callback((protocol, throwable) -> {
            if (throwable != null) {
                LOGGER.warn(throwable, "master initialize fail?");
            }

            Promises.schedule( //
                    () -> {
                        // start heart beart
                        AllocateResponse response = protocol.allocate(AllocateRequest.newInstance(
                                response_id.incrementAndGet(),
                                Float.NaN,
                                drainResrouceRequest(resources_asking),
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

    protected List<ResourceRequest> drainResrouceRequest(Queue<ResourceRequest> requests) {
        return Lists.newArrayList(Iterators.consumingIterator(requests.iterator())) //
                .parallelStream() //
                .reduce(//
                        Maps.<Resource, Integer>newConcurrentMap(), //
                        (updated, request) -> {
                            updated.put(request.getCapability(), request.getNumContainers());
                            return updated;
                        }, //
                        (left, right) -> {
                            right.entrySet().parallelStream().forEach((entry) -> {
                                left.compute(entry.getKey(), (key, old) -> {
                                    if (old == null) {
                                        old = 0;
                                    }

                                    return old + entry.getValue();
                                });
                            });
                            return left;
                        }
                ) //
                .entrySet().parallelStream() //
                .map((entry) -> ResourceRequest.newInstance(
                        Priority.UNDEFINED, //
                        ResourceRequest.ANY, //
                        entry.getKey(), //
                        entry.getValue()
                        )
                ) //
                .collect(Collectors.toList());
    }

    protected <T> ListenablePromise<T> doAs(Callable<T> callback) {
        return master.transform((protocol) -> {
            return protocol.doAS(callback);
        });
    }

    protected LoadingCache<Container, ListenablePromise<YarnNMProtocol>> buildNodeRPCProxyCache() {
        return CacheBuilder.newBuilder() //
                .expireAfterAccess(5, TimeUnit.MINUTES) //
                .removalListener((RemovalNotification<Container, ListenablePromise<YarnNMProtocol>> notice) -> {
                    notice.getValue().callback((protocol, throwable) -> {
                        if (throwable != null) {
                            LOGGER.error(throwable, "fail to stop container:" + notice.getKey());
                            return;
                        }
                        protocol.close();
                    });
                }).build(new CacheLoader<Container, ListenablePromise<YarnNMProtocol>>() {
                    @Override
                    public ListenablePromise<YarnNMProtocol> load(Container key) throws Exception {
                        return containers.transformAsync((protocol) -> {
                            return protocol.connect(key.getNodeId().getHost(), key.getNodeId().getPort());
                        });
                    }
                });
    }

    protected void masterHeartbeat(AllocateResponse response) {
        LOGGER.debug("response:" + response);
        Queue<org.apache.hadoop.security.token.Token> tokens = Queues.newConcurrentLinkedQueue();

        if (response.getAMRMToken() != null) {
            Token token = response.getAMRMToken();

            LOGGER.info("update application master token..." + token);
            org.apache.hadoop.security.token.Token<AMRMTokenIdentifier> master_toekn
                    = new org.apache.hadoop.security.token.Token<AMRMTokenIdentifier>(
                    token.getIdentifier().array(), //
                    token.getPassword().array(), //
                    new Text(token.getKind()), //
                    new Text(token.getService()) //
            );

            tokens.offer(master_toekn);
        }

        // update node token
        if (response.getNMTokens() != null) {
            response.getNMTokens().parallelStream().forEach((token) -> {
                LOGGER.info("update node manager token..." + token);
                org.apache.hadoop.security.token.Token<NMTokenIdentifier> node_manager_token =
                        ConverterUtils.convertFromYarn(token.getToken(), new InetSocketAddress(token.getNodeId().getHost(), token.getNodeId().getPort()));
                /*
                node_tokens.compute(token.getNodeId(), (key, old) -> {
                    ListenablePromise<NMToken> updated = Promises.immediate(token);
                    if (old instanceof SettablePromise) {
                        ((SettablePromise<NMToken>) old).set(token);
                    }
                    return updated;
                });
                */
                tokens.offer(node_manager_token);
            });
        }

        // assigned
        master.callback((instance, throwable) -> {
            tokens.stream().forEach(instance.ugi()::addToken);
            assign(response.getAllocatedContainers());
        });
    }

    protected void assign(List<Container> containers) {
        if (containers == null || containers.isEmpty()) {
            return;
        }

        Promises.submit(() -> doAssign(containers)).callback((remainds, throwable) -> {
            if (throwable != null) {
                LOGGER.error(throwable, "file when assining container:" + containers);
                return;
            }

            assign(remainds);
        });
    }

    protected List<Container> doAssign(List<Container> usable_container) {
        Iterator<Map.Entry<Resource, Queue<SettablePromise<Container>>>> asking = container_asking.entrySet()
                .parallelStream().sorted().iterator();
        Iterator<Container> containers = usable_container.parallelStream().sorted((left, right) -> {
            return left.getResource().compareTo(right.getResource());
        }).iterator();

        List<Container> not_assigned = Lists.newLinkedList();
        while (containers.hasNext()) {
            Container container = containers.next();
            Resource container_resoruce = container.getResource();
            SettablePromise<Container> assigned = null;

            while (asking.hasNext()) {
                Map.Entry<Resource, Queue<SettablePromise<Container>>> entry = asking.next();
                Queue<SettablePromise<Container>> waiting = entry.getValue();
                Resource ask = entry.getKey();

                // skip empty
                if (waiting.isEmpty()) {
                    continue;
                }

                // satisfied
                if (container_resoruce.compareTo(ask) >= 0) {
                    LOGGER.info("assign container:" + container + " to ask:" + ask);
                    assigned = waiting.poll();

                    // find one , stop
                    if (assigned != null) {
                        break;
                    }
                }
            }

            // do assgin
            // intended no null check
            if (assigned != null) {
                assigned.set(container);
            } else {
                not_assigned.add(container);
            }
        }

        container_asking.entrySet().stream().filter((entry) -> entry.getValue().size() > 0).forEach((entry) -> {
            LOGGER.info("waiting list:" + entry.getKey() + " size:" + entry.getValue().size());
        });
        LOGGER.info("useable container:" + usable_container + " not assigned:" + not_assigned);
        return not_assigned;
    }
}

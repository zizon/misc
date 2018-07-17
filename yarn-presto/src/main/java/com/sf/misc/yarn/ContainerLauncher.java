package com.sf.misc.yarn;

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
    protected final UserGroupInformation ugi;
    protected final YarnApplication application;
    protected final URI classloader;
    protected final ListenablePromise<YarnRPC> yarn;
    protected final Configuration configuration;

    protected final AtomicInteger response_id;
    protected final ConcurrentMap<NodeId, ListenablePromise<NMToken>> node_tokens;
    protected final LoadingCache<Container, ListenablePromise<ContainerManagementProtocol>> node_rpc_proxys;
    protected final ConcurrentMap<Resource, Queue<SettablePromise<Container>>> container_asking;
    protected final Queue<ResourceRequest> resources_asking;

    public ContainerLauncher(
            YarnApplication application,
            URI classloader
    ) {
        this.master = application.getRPCProtocol();
        this.ugi = application.getUGI();
        this.application = application;
        this.classloader = classloader;
        this.configuration = application.getConfiguration();
        this.yarn = Promises.submit(() -> YarnRPC.create(configuration));

        this.node_rpc_proxys = buildNodeRPCProxyCache();
        this.response_id = new AtomicInteger(0);
        this.node_tokens = Maps.newConcurrentMap();

        this.container_asking = Maps.newConcurrentMap();
        this.resources_asking = Queues.newConcurrentLinkedQueue();

        startHeartbeats();
    }

    public ListenablePromise<Container> launchContainer(Class<?> entry_class, Resource resource) {
        return this.launchContainer(entry_class, resource, null, null);
    }

    public ListenablePromise<Container> launchContainer(Class<?> entry_class, Resource
            resource, Map<String, String> properties, Map<String, String> enviroments) {
        // for jvm overhead,increase resource demand
        int rounded_memory = rounded_memory = resource.getMemory();
        Resource demand = Resource.newInstance((int) (rounded_memory * 1.2), resource.getVirtualCores());

        // allocated notifier
        SettablePromise<Container> allocated_container = SettablePromise.create();

        // launched notifier
        ListenablePromise<Container> launched_container = allocated_container.transformAsync((allocated) -> {
            return node_rpc_proxys.get(allocated).transformAsync((node) -> {
                StartContainersResponse response = node.startContainers(
                        StartContainersRequest.newInstance( //
                                Collections.singletonList( //
                                        StartContainerRequest.newInstance(
                                                createContext( //
                                                        demand, //
                                                        entry_class, //
                                                        Optional.ofNullable(properties).orElse(Collections.emptyMap()), //
                                                        Optional.ofNullable(enviroments).orElse(Collections.emptyMap()) //
                                                ), //
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

        container_asking.compute(demand, (key, old) -> {
            if (old == null) {
                old = Queues.newConcurrentLinkedQueue();
            }

            old.offer(allocated_container);
            return old;
        });

        return application.getApplication().transformAsync(
                (_ignore) -> {
                    int id = response_id.incrementAndGet();
                    LOGGER.info("request container:" + entry_class + " resource:" + resource + " response_id:" + id);
                    resources_asking.offer(ResourceRequest.newInstance( //
                            Priority.UNDEFINED, //
                            ResourceRequest.ANY, //
                            demand, //
                            1 //
                    ));
                    return launched_container;
                }
        );
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

    protected void startHeartbeats() {
        application.getApplication().transformAsync((ignore) -> master).callback((master, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error(throwable, "fiail when try to register master");
                        return;
                    }

                    Promises.schedule( //
                            () -> {
                                masterHeartbeat(master.allocate(AllocateRequest.newInstance(
                                        response_id.incrementAndGet(),
                                        Float.NaN,
                                        drainResrouceRequest(resources_asking),
                                        Collections.emptyList(),
                                        ResourceBlacklistRequest.newInstance(
                                                Collections.emptyList(), //
                                                Collections.emptyList() //
                                        ))
                                ));
                            }, //
                            TimeUnit.SECONDS.toMillis(5) //
                    );
                }
        );
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


    protected LoadingCache<Container, ListenablePromise<ContainerManagementProtocol>> buildNodeRPCProxyCache() {
        return CacheBuilder.newBuilder() //
                .expireAfterAccess(5, TimeUnit.MINUTES) //
                .removalListener((RemovalNotification<Container, ListenablePromise<ContainerManagementProtocol>> notice) -> {
                    Promises.chain(notice.getValue(), yarn).call((protocol, yarn) -> {
                        return ugi.doAs((PrivilegedExceptionAction<?>) () -> {
                            yarn.stopProxy(protocol, configuration);
                            return 0;
                        });
                    }).callback((ignore, throwable) -> {
                        if (throwable != null) {
                            LOGGER.error(throwable, "fail to stop container:" + notice.getKey());
                        }
                    });
                }).build(new CacheLoader<Container, ListenablePromise<ContainerManagementProtocol>>() {
                    @Override
                    public ListenablePromise<ContainerManagementProtocol> load(Container key) throws Exception {
                        return Promises.<ApplicationId, YarnRPC, ContainerManagementProtocol>chain(application.getApplication(), yarn).call((appid, protocol) -> {
                            return ugi.doAs( //
                                    (PrivilegedExceptionAction<ContainerManagementProtocol>) () -> (ContainerManagementProtocol) protocol.getProxy( //
                                            ContainerManagementProtocol.class, //
                                            new InetSocketAddress( //
                                                    key.getNodeId().getHost(), //
                                                    key.getNodeId().getPort() //
                                            ),
                                            configuration //
                                    ) //
                            );
                        });
                    }
                });
    }

    protected void masterHeartbeat(AllocateResponse response) {
        LOGGER.info("response:" + response);
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
            ugi.addToken(master_toekn);
        }

        // update node token
        if (response.getNMTokens() != null) {
            response.getNMTokens().parallelStream().forEach((token) -> {
                LOGGER.info("update node manager token..." + token);
                org.apache.hadoop.security.token.Token<NMTokenIdentifier> node_manager_token =
                        ConverterUtils.convertFromYarn(token.getToken(), new InetSocketAddress(token.getNodeId().getHost(), token.getNodeId().getPort()));
                node_tokens.compute(token.getNodeId(), (key, old) -> {
                    ListenablePromise<NMToken> updated = Promises.immediate(token);
                    if (old instanceof SettablePromise) {
                        ((SettablePromise<NMToken>) old).set(token);
                    }
                    return updated;
                });
                ugi.addToken(node_manager_token);
            });
        }

        // assigned
        assign(response.getAllocatedContainers());
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

    protected List<String> launchCommand(Resource resource, Map<String, String> properties) {
        // launch command
        List<String> commands = Lists.newLinkedList();
        commands.add(ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java");

        // heap tuning
        commands.add("-Xmx" + resource.getMemory() + "M");
        if (resource.getMemory() > 5 * 1024) {
            commands.add("-XX:+UseG1GC");
        } else {
            commands.add("-XX:+UseParallelGC");
            commands.add("-XX:+UseParallelOldGC");
        }

        // gc log
        Arrays.asList("-XX:+PrintGCDateStamps", //
                "-verbose:gc", //
                "-XX:+PrintGCDetails", //
                "-Xloggc:" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/gc.log", //
                "-XX:+UseGCLogFileRotation", //
                "-XX:NumberOfGCLogFiles=10", //
                "-XX:GCLogFileSize=8M" //
        ).stream().sequential().forEach((command) -> commands.add(command));

        // pass properties
        properties.entrySet().stream().sequential() //
                .forEach((entry) -> {
                    commands.add("-D" + entry.getKey() + "=" + entry.getValue());
                });

        // entry class
        commands.add(KickStart.class.getName());

        // logs
        commands.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/" + ApplicationConstants.STDOUT);
        commands.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/" + ApplicationConstants.STDERR);

        commands.add(";\n");
        return commands;
    }

    protected List<String> prepareJar(String http_classloader) {
        List<String> command = Lists.newLinkedList();

        String meta_inf = "META-INF";
        String manifest = "MANIFEST.MF";

        // prepare jar dir
        command.addAll(Arrays.asList("mkdir", "-p", meta_inf));
        command.add(";\n");

        // touch
        command.addAll(Arrays.asList("touch", meta_inf + "/" + manifest));
        command.add(";\n");

        // write
        command.addAll(Arrays.asList("echo", "'Manifest-Version: 1.0'", ">" + meta_inf + "/" + manifest));
        command.add(";\n");

        command.addAll(Arrays.asList("echo", "'Class-Path: " + http_classloader + "'", ">>" + meta_inf + "/" + manifest));
        command.add(";\n");

        // zip
        command.addAll(Arrays.asList("zip", "-r", "_kickstart_.jar", meta_inf));
        command.add(";\n");

        return command;
    }

    protected ContainerLaunchContext createContext(Resource resource, Class<?>
            entry_class, Map<String, String> properties, Map<String, String> enviroment) throws MalformedURLException {
        /*
        // instruct update resoruces
        Map<String, LocalResource> local_resoruce = Maps.newTreeMap();

        // copy
        properties = Maps.newHashMap(properties);
        enviroment = Maps.newHashMap(enviroment);

        // prepare need info
        // http classlaoder server url
        enviroment.put(KickStart.HTTP_CLASSLOADER_URL, //
                classloader.toURL().toExternalForm()
                /*,
                new URL(classloader.toURL(), //
                        HttpClassloaderResource.class.getAnnotation(javax.ws.rs.Path.class).value() + "/" //
                ).toExternalForm()

        );

        // the entry class that run in container
        enviroment.put(KickStart.KICKSTART_CLASS, entry_class.getName());

        // add bootstrap classpath
        enviroment.put(ApplicationConstants.Environment.CLASSPATH.key(), ".:./*");
        enviroment.put(Enviroments.CONTAINER_LOG_DIR.name(), ApplicationConstants.LOG_DIR_EXPANSION_VAR);

        List<String> commands = Lists.newLinkedList();

        // jar
        commands.addAll(prepareJar(enviroment.get(KickStart.HTTP_CLASSLOADER_URL)));

        // launcer
        commands.addAll(launchCommand(resource, properties));

        ContainerLaunchContext context = ContainerLaunchContext.newInstance(local_resoruce, //
                enviroment, //
                commands, //
                null, //
                null, //
                null);

        return context;
         */
        return null;
    }
}

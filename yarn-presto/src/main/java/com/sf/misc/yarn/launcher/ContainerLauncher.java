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
import com.google.common.collect.Sets;
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
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.exceptions.ApplicationMasterNotRegisteredException;
import org.apache.hadoop.yarn.exceptions.InvalidApplicationMasterRequestException;
import org.apache.hadoop.yarn.security.NMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public abstract class ContainerLauncher {
    public static final Logger LOGGER = Logger.get(ContainerLauncher.class);

    protected final ListenablePromise<YarnRMProtocol> master_service;
    protected final ListenablePromise<LauncherEnviroment> enviroment;

    protected final AtomicInteger request_sequence;
    protected final ConcurrentMap<NodeId, ListenablePromise<NMToken>> node_tokens;
    protected final LoadingCache<Container, ListenablePromise<YarnNMProtocol>> container_services;
    protected final ConcurrentMap<Resource, Queue<SettablePromise<Container>>> container_asking;
    protected final Queue<Resource> resource_asking;
    protected final Set<Container> free_containers;

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
        this.free_containers = Sets.newConcurrentHashSet();

        if (!nohearbeat) {
            startHeartbeats();
        }
    }

    protected abstract ListenablePromise<RegisterApplicationMasterResponse> registerMaster();

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

    public ListenablePromise<ContainerId> releaseContainer(ContainerId container) {
        return this.master_service.transformAsync((master) -> {
            return releaseContainer( //
                    toContainer( //
                            master.getContainerReport( //
                                    GetContainerReportRequest.newInstance(container) //
                            ).getContainerReport() //
                    ) //
            );
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
        if (container_config.classloader() == null) {
            // if container config specifiyed no classloader,use this container launcher`s
            return enviroment.transformAsync((enviroment) -> {
                return enviroment.classloader;
            }).transformAsync((classloader) -> {
                container_config.updateCloassloader(classloader.toURL().toExternalForm());
                return createContext(container_config);
            });
        }

        return enviroment.transformAsync((launcher) -> {
            return launcher.launcherCommand(
                    Resource.newInstance(container_config.getMemory(), container_config.getCpu()), //
                    Class.forName(container_config.getMaster()) //
            ).transform((commands) -> {
                Map<String, String> combinde_enviroments = Maps.newHashMap();

                // container config
                // add this airlift config
                combinde_enviroments.put(LauncherEnviroment.CONTAINER_CONFIGURATION, ContainerConfiguration.encode(container_config));

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
        LOGGER.info("asking contaienr:" + container_config.getMaster()
                + " with memory:" + container_config.getMemory()
                + " cpu:" + container_config.getCpu()
                + " configuration:\n" + container_config.configs().entrySet().parallelStream()
                .map((entry) -> "config:" + entry.getKey() + " value:" + entry.getValue())
                .collect(Collectors.joining("\n"))
        );
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
                        container_config.getMemory(), //
                        container_config.getCpu() //
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

    public ListenablePromise<ContainerId> masterContianer(ApplicationAttemptId attempt_id) {
        return master_service.transform((master) -> {
            return master.getApplicationAttemptReport(GetApplicationAttemptReportRequest.newInstance(attempt_id))
                    .getApplicationAttemptReport().getAMContainerId();
        });
    }

    public ListenablePromise<List<ContainerReport>> listContainer(ApplicationAttemptId attempt_id) {
        return Promises.retry(() -> {
            // ensure no pending heartbeat
            boolean pending_request = !resource_asking.isEmpty();

            if (pending_request) {
                LOGGER.info("pending container request,backoff container listing...");
                return Optional.empty();
            }

            // no pending reqeust,list container
            ListenablePromise<List<ContainerReport>> containers = master_service.transform((master) -> {
                return master.getContainers(GetContainersRequest.newInstance(attempt_id)) //
                        .getContainerList().parallelStream()
                        .collect(Collectors.toList());
            });
            return Optional.of(containers);
        }).transformAsync((through) -> through);
    }

    protected ListenablePromise<YarnRMProtocol> masterService() {
        return this.master_service;
    }

    protected Container toContainer(ContainerReport report) {
        return Container.newInstance(
                report.getContainerId(),
                report.getAssignedNode(),
                report.getNodeHttpAddress(),
                report.getAllocatedResource(),
                report.getPriority(),
                null
        );
    }

    protected void startHeartbeats() {
        // active register
        registerMaster().logException((ignore) -> "fail to active register master,try latter in heartbeart");

        // start heart beats
        master_service.callback((master_service) -> {
            Promises.schedule( //
                    () -> {
                        // save reqeust,and do not forget to push it back when fail occurs
                        List<ResourceRequest> requests = drainResrouceRequest();
                        try {
                            // start heart beart
                            AllocateResponse response = master_service.allocate(AllocateRequest.newInstance(
                                    request_sequence.incrementAndGet(),
                                    Float.NaN,
                                    requests,
                                    Collections.emptyList(),
                                    ResourceBlacklistRequest.newInstance(
                                            Collections.emptyList(), //
                                            Collections.emptyList() //
                                    ))
                            );

                            // preocess it
                            masterHeartbeat(response);
                        } catch (Throwable exception) {
                            LOGGER.warn("adding back resource request:" + requests);

                            // recover request
                            requests.parallelStream()
                                    // to resoruce
                                    .flatMap((request) -> IntStream.range(0, request.getNumContainers())
                                            .mapToObj((ignore) -> request.getCapability()) //
                                    )
                                    // add back
                                    .forEach(resource_asking::offer);

                            if (matchMasterException(exception, ApplicationMasterNotRegisteredException.class)) {
                                registerMaster().logException((reason) -> "fail to register applicaiton master");
                                return;
                            } else if (matchMasterException(exception, InvalidApplicationMasterRequestException.class)) {
                                LOGGER.warn(exception, "invalid application master reqeust");
                                return;
                            }

                            LOGGER.error(exception, "fail when doing heartbeart...");
                        }
                    }, //
                    TimeUnit.SECONDS.toMillis(5), //
                    true
            );
        }).logException((ignore) -> "fail to start container launcher heartbeart");
    }

    protected boolean matchMasterException(Throwable throwable, Class<?> exception) {
        do {
            if (exception.isAssignableFrom(throwable.getClass())) {
                return true;
            }

            throwable = throwable.getCause();
        } while (throwable != null);

        return false;
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
                                int num_of_containers = value == null ? 0 : value.getNumContainers();
                                value = ResourceRequest.newInstance(
                                        Priority.UNDEFINED,
                                        ResourceRequest.ANY,
                                        key,
                                        num_of_containers + 1
                                );
                                return value;
                            });
                        },
                        (left, right) -> {
                            // merge left and right
                            right.entrySet().parallelStream() //
                                    .forEach((right_entry) -> {
                                        // merge contaienrs with same resource ask
                                        left.compute(right_entry.getKey(), (resource, left_containers) -> {
                                            ResourceRequest request = right_entry.getValue();

                                            // left is not null
                                            if (left_containers != null) {
                                                request.setNumContainers(request.getNumContainers() + left_containers.getNumContainers());
                                            }

                                            return request;
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
        LOGGER.debug("preocess heartbeat, new containers:" + response.getAllocatedContainers().size());

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
        master_service.callback((master_service) -> {
            tokens.stream().forEach(master_service.ugi()::addToken);

            // collect not assigned containers
            response.getAllocatedContainers().parallelStream()
                    .map(this::ensureContainerState)
                    .forEach((optional) -> {
                        optional.callback((container) -> {
                            container.ifPresent(free_containers::add);
                        }).logException();
                    });

            // assign
            assign();
        });
    }

    protected void assign() {
        if (free_containers == null || free_containers.isEmpty()) {
            return;
        }

        Promises.submit(() -> doAssign()).logException();
    }

    protected ListenablePromise<Boolean> doAssignOneContainer(Container container) {
        return Promises.submit(() -> {
            Optional<Map.Entry<Resource, Queue<SettablePromise<Container>>>> optianl_asking = container_asking.entrySet().parallelStream()
                    // find someone that asking
                    .filter((entry) -> !entry.getValue().isEmpty())
                    // find someone that this container is capable
                    .filter((entry) -> container.getResource().compareTo(entry.getKey()) >= 0)
                    .sorted(Comparator.comparing(Map.Entry::getKey))
                    // find the minimal resource asking,
                    // that is the most fitted one
                    .findFirst();

            if (!optianl_asking.isPresent()) {
                return false;
            }

            Map.Entry<Resource, Queue<SettablePromise<Container>>> asking = optianl_asking.get();
            SettablePromise<Container> ask = asking.getValue().poll();
            if (ask != null) {
                boolean ok = ask.set(container);
                if (ok) {
                    LOGGER.info("assign container:" + container + " to ask:" + asking.getKey());
                    return true;
                } else {
                    LOGGER.warn("try assign assigned container:" + container + " to ask:" + asking.getKey() + " ,give up");
                    return false;
                }
            }

            LOGGER.warn("find no asking for this contaienr:" + container + " ,may be in race");
            return false;
        });
    }

    protected void doAssign() {
        Lists.newArrayList(Iterators.consumingIterator(free_containers.iterator())).parallelStream()
                .forEach((container) -> {
                    doAssignOneContainer(container).callback((assigned) -> {
                        if (!assigned) {
                            LOGGER.warn("container:" + container + " not assign, add back to free contaienr set");
                            free_containers.add(container);
                        }
                    }).logException((ignore) -> {
                        free_containers.add(container);
                        return "try to assign container:" + container + " fail, and adding back to free container set";
                    });
                });

        // log context
        container_asking.entrySet().stream().filter((entry) -> !entry.getValue().isEmpty()).forEach((entry) -> {
            LOGGER.info("waiting list:" + entry.getKey() + " size:" + entry.getValue().size());
        });

        free_containers.parallelStream().forEach((container) -> {
            LOGGER.info("not assign:" + container);
        });
    }

    protected ListenablePromise<Optional<Container>> ensureContainerState(Container container) {
        return master_service.transform((master_service) -> {
            GetContainerReportResponse response = master_service.getContainerReport(GetContainerReportRequest.newInstance(container.getId()));

            LOGGER.info("allocated container:" + container + " report:" + response.getContainerReport());

            switch (response.getContainerReport().getContainerState()) {
                case NEW:
                case RUNNING:
                    return Optional.of(container);
                case COMPLETE:
                default:
                    LOGGER.warn("container already in complete state:" + response.getContainerReport());
                    return Optional.empty();
            }
        });
    }
}

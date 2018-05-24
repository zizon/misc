package com.sf.misc.yarn;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import com.sf.misc.annotaions.ForOnYarn;
import com.sf.misc.async.ExecutorServices;
import com.sf.misc.classloaders.HttpClassloaderResource;
import com.sf.misc.classloaders.JarCreator;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.util.ConverterUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class ContainerLauncher extends YarnCallbackHandler {
    public static final Logger LOGGER = Logger.get(ContainerLauncher.class);

    public static final String KICKSTART_JAR_FILE_NAME = "_kickstart_.jar";

    public static enum Enviroments {
        CONTAINER_LOG_DIR;
    }

    protected AMRMClientAsync master;
    protected NMClientAsync nodes;
    protected YarnClient yarn;
    protected FileSystem hdfs;
    protected HttpServerInfo server_info;
    protected DataSize minimum_resource;
    protected Path work_dir;

    protected ConcurrentMap<Resource, Queue<SettableFuture<Container>>> container_asking;
    protected ConcurrentMap<ContainerId, SettableFuture<ContainerStatus>> container_released;
    protected ConcurrentMap<ContainerId, Consumer<Throwable>> container_starting;
    protected ConcurrentMap<ContainerId, SettableFuture<ContainerStatus>> container_query;

    protected LoadingCache<ApplicationId, ListenableFuture<Path>> kickstart_jars;
    protected AtomicInteger counter;

    @Inject
    public ContainerLauncher(@ForOnYarn AMRMClientAsync master, @ForOnYarn NMClientAsync nodes, FileSystem hdfs, HttpServerInfo server_info, HadoopConfig hadoop_config, @ForOnYarn YarnClient yarn) {
        this.master = master;
        this.nodes = nodes;
        this.hdfs = hdfs;
        this.yarn = yarn;
        this.server_info = server_info;
        this.work_dir = new Path(hadoop_config.getWorkDir());

        this.container_asking = Maps.newConcurrentMap();
        this.container_released = Maps.newConcurrentMap();
        this.container_starting = Maps.newConcurrentMap();
        this.container_query = Maps.newConcurrentMap();

        this.kickstart_jars = CacheBuilder.newBuilder() //
                .expireAfterAccess(1, TimeUnit.MINUTES) //
                .build(CacheLoader.from(this::prepareKickStartJar));

        this.counter = new AtomicInteger(1);

        this.minimum_resource = hadoop_config.getMinimunResource();
    }

    public ListenableFuture<Container> launchContainer(ApplicationId appid, Class<?> entry_class, Resource resource) {
        return this.launchContainer(appid, entry_class, resource, null, null);
    }

    public ListenableFuture<Container> launchContainer(ApplicationId appid, Class<?> entry_class, Resource resource, Map<String, String> properties, Map<String, String> enviroments) {
        // for jvm overhead,increase resource demand
        int rounded_memory = Math.min(resource.getMemory(), (int) minimum_resource.getValue());
        Resource demand = Resource.newInstance((int) (rounded_memory * 1.2), resource.getVirtualCores());
        /*
        SchedulerUtils.normalizeRequests(ask, new DominantResourceCalculator(),
                clusterResource, minimumAllocation, maximumAllocation, incrAllocation);
                */

        // ask contaienr
        SettableFuture<Container> ask = SettableFuture.create();
        this.container_asking.compute(demand, (key, old) -> {
            if (old == null) {
                old = Queues.newConcurrentLinkedQueue();
            }

            if (!old.offer(ask)) {
                throw new IllegalStateException("fail to ask resource:" + resource + " for application:" + appid + " of class:" + entry_class);
            }
            return old;
        });

        // request
        AMRMClient.ContainerRequest request = new AMRMClient.ContainerRequest(demand, null, null, Priority.UNDEFINED);
        LOGGER.info("send a container request:" + request);
        this.master.addContainerRequest(request);

        // prepare kickstart jar
        ListenableFuture<ContainerLaunchContext> context_initializing = Futures.transform(
                this.kickstart_jars.getUnchecked(appid),  //
                (jar_output) -> {
                    try {
                        return createContext( //
                                appid, //
                                jar_output,//
                                resource,
                                entry_class, //
                                Optional.ofNullable(properties).orElse(Maps.newTreeMap()), //
                                Optional.ofNullable(enviroments).orElse(Maps.newTreeMap()) //
                        );
                    } catch (IOException exception) {
                        throw new RuntimeException("fail when createing container context for appid:" + appid //
                                + " with jar:" + jar_output //
                                + " giving class:" + entry_class //
                                + " properties:" + properties //
                                + " enviroments:" + enviroments, //
                                exception //
                        );
                    }
                }, ExecutorServices.executor());

        // then launch
        return Futures.transformAsync(ask, (container) -> {
            // set future
            SettableFuture<Container> future = SettableFuture.create();
            this.container_starting.compute(container.getId(), (key, old) -> {
                if (old == null) {
                    old = (exception) -> {
                        if (exception != null) {
                            future.setException(exception);
                        } else {
                            future.set(container);
                        }
                    };
                }

                return old;
            });

            // start
            return Futures.transformAsync(context_initializing, (context) -> {
                this.nodes.startContainerAsync(container, context);
                return future;
            }, ExecutorServices.executor());
        }, ExecutorServices.executor());
    }

    public ListenableFuture<ContainerStatus> releaseContainer(Container container) {
        // set future
        return this.container_released.compute(container.getId(), (key, release_future) -> {
            // not released yet
            if (release_future == null) {
                release_future = SettableFuture.create();
                this.nodes.stopContainerAsync(container.getId(), container.getNodeId());
            }

            // container complete before explict invoke release
            return release_future;
        });
    }

    public ListenableFuture<ContainerStatus> containerCompletion(ContainerId container_id) {
        return this.container_released.compute(container_id, (key, release_future) -> {
            return Optional.ofNullable(release_future) //
                    .orElse(SettableFuture.create());
        });
    }

    public ListenableFuture<ContainerStatus> containerStatus(Container container) {
        ListenableFuture<ContainerStatus> completed = this.container_released.get(container.getId());
        if (completed != null) {
            return completed;
        }

        // prepare future
        ListenableFuture<ContainerStatus> query_future = this.container_query.compute(container.getId(), (key, presented) -> {
            if (presented == null) {
                presented = SettableFuture.create();
            }

            return presented;
        });

        // send request
        this.nodes.getContainerStatusAsync(container.getId(), container.getNodeId());
        return query_future;
    }

    public void garbageCollectWorkDir() {
        try {
            LOGGER.info("start cleanup work dir:" + work_dir);
            RemoteIterator<FileStatus> files = this.hdfs.listStatusIterator(work_dir);
            while (files.hasNext()) {
                FileStatus status = files.next();
                LOGGER.info("process file:" + status);
                if (!status.isDirectory()) {
                    LOGGER.info("skip not dir:" + status);
                    continue;
                }

                // a directory
                Optional<ApplicationId> appid = parseApplicationID(status.getPath().getName());
                if (!appid.isPresent()) {
                    LOGGER.info("skip not appid:" + status.getPath().getName());
                    continue;
                }

                // parse ok
                Futures.addCallback(ExecutorServices.executor().submit(() -> {
                            return yarn.getApplicationReport(appid.get());
                        }), //
                        new FutureCallback<ApplicationReport>() {
                            @Override
                            public void onSuccess(@Nullable ApplicationReport result) {
                                if (result.getFinalApplicationStatus() != FinalApplicationStatus.UNDEFINED) {
                                    // finished
                                    try {
                                        LOGGER.info("cleaning up workdir:" + status.getPath() + " for application:" + appid.get());
                                        hdfs.delete(status.getPath(), true);
                                    } catch (IOException e) {
                                        LOGGER.warn(e, "unexpected io exception when cleaning up:" + status.getPath());
                                    }
                                }
                            }

                            @Override
                            public void onFailure(Throwable t) {
                                LOGGER.warn(t, "fail to get application report:" + appid.get());
                                // finished
                                try {
                                    LOGGER.info("cleaning up workdir:" + status.getPath() + " for application:" + appid.get());
                                    hdfs.delete(status.getPath(), true);
                                } catch (IOException e) {
                                    LOGGER.warn(e, "unexpected io exception when cleaning up:" + status.getPath());
                                }
                            }
                        }, ExecutorServices.executor());
            }
            LOGGER.info("clean done");
        } catch (IOException e) {
            LOGGER.error(e);
            //throw new UncheckedIOException(e);
        }
    }

    protected String stringifyApplicationID(ApplicationId appid) {
        return ApplicationId.appIdStrPrefix + appid.getClusterTimestamp() + "_" + appid.getId();
    }

    protected Optional<ApplicationId> parseApplicationID(String raw) {
        LOGGER.info("parsing appliction string:" + raw);
        if (!raw.startsWith(ApplicationId.appIdStrPrefix)) {
            LOGGER.warn("application string:" + raw + " not starts with:" + ApplicationId.appIdStrPrefix);
            return Optional.empty();
        }

        String[] tuple = raw.substring(ApplicationId.appIdStrPrefix.length()).split("_");
        if (tuple.length != 2) {
            LOGGER.warn("application string:" + raw + " not compose by clustertimestampe and id");
        }

        try {
            return Optional.of(ApplicationId.newInstance(Long.parseLong(tuple[0]), Integer.parseInt(tuple[1])));
        } catch (NumberFormatException e) {
            LOGGER.warn(e, "application string:" + raw + " number format error");
            return Optional.empty();
        }
    }

    protected ListenableFuture<Path> prepareKickStartJar(ApplicationId appid) {
        try {
            // ensure root
            Path workdir = new Path(this.work_dir, stringifyApplicationID(appid));
            hdfs.mkdirs(workdir);

            // create jar
            Path jar_output = new Path(workdir, KICKSTART_JAR_FILE_NAME);
            if (!hdfs.exists(jar_output)) {
                try (OutputStream output = hdfs.create(jar_output);) {
                    new JarCreator().add(KickStart.class).write(output);
                }
            }

            return Futures.immediateFuture(jar_output);
        } catch (IOException exception) {
            return Futures.immediateFailedFuture(exception);
        }
    }

    protected ContainerLaunchContext createContext(ApplicationId appid, Path jar_output, Resource resource, Class<?> entry_class, Map<String, String> properties, Map<String, String> enviroment) throws IOException {
        FileStatus status = hdfs.getFileStatus(jar_output);
        // instruct update resoruces
        Map<String, LocalResource> local_resoruce = Maps.newTreeMap();
        local_resoruce.put("kickstart.jar", LocalResource.newInstance(
                ConverterUtils.getYarnUrlFromPath(status.getPath()), //
                LocalResourceType.FILE, //
                LocalResourceVisibility.APPLICATION,//
                status.getLen(),//
                status.getModificationTime() //
                )
        );

        // prepare need info
        // http classlaoder server url
        enviroment.put(KickStart.HTTP_CLASSLOADER_URL, //
                new URL(server_info.getHttpUri().toURL(), //
                        HttpClassloaderResource.class.getAnnotation(javax.ws.rs.Path.class).value() + "/" //
                ).toExternalForm()
        );

        // the entry class that run in container
        enviroment.put(KickStart.KICKSTART_CLASS, entry_class.getName());

        // add bootstrap classpath
        enviroment.put(ApplicationConstants.Environment.CLASSPATH.key(), ".:./*");
        enviroment.put(Enviroments.CONTAINER_LOG_DIR.name(), ApplicationConstants.LOG_DIR_EXPANSION_VAR);

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

        ContainerLaunchContext context = ContainerLaunchContext.newInstance(local_resoruce, //
                enviroment, //
                commands, //
                null, //
                null, //
                null);

        return context;
    }

    @Override
    public void onContainersCompleted(List<ContainerStatus> statuses) {
        super.onContainersCompleted(statuses);

        statuses.stream().parallel().forEach((status) -> {
            this.container_released.compute(status.getContainerId(), (key, listening_completion) -> {
                if (listening_completion == null) {
                    // not explict released
                    listening_completion = SettableFuture.create();
                }

                // set completion
                listening_completion.set(status);
                return listening_completion;
            });
        });
    }

    @Override
    public void onContainersAllocated(List<Container> newly_containers) {
        super.onContainersAllocated(newly_containers);

        // sort once
        newly_containers = newly_containers.stream().parallel() //
                .sorted((left, right) -> left.getResource().compareTo(right.getResource())) //
                .collect(Collectors.toList());

        // pre sort
        List<Map.Entry<Resource, Queue<SettableFuture<Container>>>> asking = this.container_asking.entrySet().stream().parallel()//
                .sorted((left, right) -> left.getKey().compareTo(right.getKey())) //
                .collect(Collectors.toList());

        LOGGER.info("not assign containers:" + newly_containers + " asking:" + this.container_asking);
        while (!newly_containers.isEmpty()) {
            // try assign
            newly_containers = newly_containers.stream().parallel() //
                    .filter((container) -> {
                        Map.Entry<AMRMClient.ContainerRequest, SettableFuture<Container>> selected = this.container_asking.entrySet().stream().parallel()//
                                .filter((entry) -> !entry.getValue().isEmpty() && entry.getKey().compareTo(container.getResource()) <= 0) //
                                .min(Comparator.comparing((entry) -> entry.getKey())) //
                                .map((entry) -> {
                                    // decrease demand
                                    AMRMClient.ContainerRequest request = new AMRMClient.ContainerRequest(entry.getKey(), null, null, Priority.UNDEFINED);
                                    SettableFuture<Container> polled = entry.getValue().poll();

                                    return new Map.Entry<AMRMClient.ContainerRequest, SettableFuture<Container>>() {
                                        @Override
                                        public AMRMClient.ContainerRequest getKey() {
                                            return request;
                                        }

                                        @Override
                                        public SettableFuture<Container> getValue() {
                                            return polled;
                                        }

                                        @Override
                                        public SettableFuture<Container> setValue(SettableFuture<Container> value) {
                                            throw new UnsupportedOperationException("can not set for allocated entry");
                                        }
                                    };
                                }) //
                                .orElse(null);

                        if (selected != null && selected.getValue() != null) {
                            selected.getValue().set(container);
                            master.removeContainerRequest(selected.getKey());
                            return false;
                        }

                        // no matching reqeust resource
                        return true;
                    })//
                    .collect(Collectors.toList());

        }
        LOGGER.info("all assigned");
    }

    @Override
    public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
        super.onContainerStarted(containerId, allServiceResponse);

        this.container_starting.compute(containerId, (key, callback) -> {
            callback.accept(null);
            return null;
        });
    }

    @Override
    public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
        super.onContainerStatusReceived(containerId, containerStatus);

        this.container_query.compute(containerId, (key, future) -> {
            if (future != null) {
                future.set(containerStatus);
            }
            return null;
        });
    }

    @Override
    public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
        super.onGetContainerStatusError(containerId, t);

        this.container_query.compute(containerId, (key, future) -> {
            if (future != null) {
                future.setException(t);
            }
            return null;
        });
    }

    @Override
    public void onStartContainerError(ContainerId containerId, Throwable t) {
        super.onStartContainerError(containerId, t);

        this.container_starting.compute(containerId, (key, callback) -> {
            callback.accept(t);
            return null;
        });
    }
}

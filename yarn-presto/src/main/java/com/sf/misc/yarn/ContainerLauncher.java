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
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
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

            this.nodes.startContainerAsync(container, //
                    createContext( //
                            appid, //
                            resource,
                            entry_class, //
                            Optional.ofNullable(properties).orElse(Maps.newTreeMap()), //
                            Optional.ofNullable(enviroments).orElse(Maps.newTreeMap()) //
                    ) //
            );

            return future;
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

    protected ContainerLaunchContext createContext(ApplicationId appid, Resource resource, Class<?> entry_class, Map<String, String> properties, Map<String, String> enviroment) throws MalformedURLException {
        // instruct update resoruces
        Map<String, LocalResource> local_resoruce = Maps.newTreeMap();

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

package com.sf.misc.yarn;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ContainerLauncher extends YarnCallbackHandler {
    public static final Logger LOGGER = Logger.get(ContainerLauncher.class);

    public static final String KICKSTART_JAR_FILE_NAME = "_kickstart_.jar";

    protected AMRMClientAsync master;
    protected NMClientAsync nodes;
    protected FileSystem hdfs;
    protected HttpServerInfo server_info;

    protected ConcurrentMap<Resource, Queue<SettableFuture<Container>>> resource_reqeusted;
    protected ConcurrentMap<ContainerId, SettableFuture<ContainerStatus>> container_released;
    protected ConcurrentMap<ContainerId, ExecutorServices.Lambda> container_launching;

    protected LoadingCache<ApplicationId, ListenableFuture<Path>> kickstart_jars;

    @Inject
    public ContainerLauncher(@ForOnYarn AMRMClientAsync master, @ForOnYarn NMClientAsync nodes, FileSystem hdfs, HttpServerInfo server_info) {
        this.master = master;
        this.nodes = nodes;
        this.hdfs = hdfs;
        this.server_info = server_info;

        this.resource_reqeusted = Maps.newConcurrentMap();
        this.container_released = Maps.newConcurrentMap();
        this.container_launching = Maps.newConcurrentMap();

        this.kickstart_jars = CacheBuilder.newBuilder() //
                .expireAfterAccess(1, TimeUnit.MINUTES) //
                .build(CacheLoader.from(this::prepareKickStartJar));
    }

    public ListenableFuture<Container> launchContainer(ApplicationId appid, Class<?> entry_class, Resource resource) {
        return this.launchContainer(appid, entry_class, resource, null, null);
    }

    public ListenableFuture<Container> launchContainer(ApplicationId appid, Class<?> entry_class, Resource resource, Map<String, String> properties, Map<String, String> enviroments) {
        // for jvm overhead,increase resource demand
        Resource demand = Resource.newInstance((int) (resource.getMemory() * 1.2), resource.getVirtualCores());

        // request contaienr
        SettableFuture<Container> reqeust = SettableFuture.create();
        this.resource_reqeusted.compute(demand, (key, old) -> {
            if (old == null) {
                old = Lists.newLinkedList();
            }

            old.add(reqeust);

            // request
            AMRMClient.ContainerRequest request = new AMRMClient.ContainerRequest(demand, null, null, Priority.UNDEFINED);
            this.master.addContainerRequest(request);
            return old;
        });

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
                });

        // then launch
        return Futures.transformAsync(reqeust, (container) -> {
            // set future
            SettableFuture<Container> future = SettableFuture.create();
            this.container_launching.putIfAbsent(container.getId(), () -> future.set(container));

            // start
            return Futures.transformAsync(context_initializing, (context) -> {
                this.nodes.startContainerAsync(container, context);
                return future;
            });
        });
    }

    public ListenableFuture<ContainerStatus> releaseContainer(Container container) {
        // set future
        SettableFuture<ContainerStatus> future = SettableFuture.create();
        this.container_released.compute(container.getId(), (key, release_future) -> {
            // not released yet
            if (release_future == null) {
                this.nodes.stopContainerAsync(container.getId(), container.getNodeId());
                return future;
            }

            // container complete before explict invoke release
            future.setFuture(release_future);
            return release_future;
        });

        return future;
    }

    public ListenableFuture<ContainerStatus> containerCompletion(ContainerId container_id) {
        return this.container_released.compute(container_id, (key, release_future) -> {
            return Optional.ofNullable(release_future) //
                    .orElse(SettableFuture.create());
        });
    }

    protected ListenableFuture<Path> prepareKickStartJar(ApplicationId appid) {
        try {
            // ensure root
            Path workdir = new Path("/tmp/unmanaged/" + appid);
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
            this.container_released.compute(status.getContainerId(), (key, release_future) -> {
                SettableFuture<ContainerStatus> immediate = SettableFuture.create();
                immediate.set(status);

                if (release_future == null) {
                    // not explict released
                    return immediate;
                }

                release_future.setFuture(immediate);
                return release_future;
            });
        });
    }

    @Override
    public void onContainersAllocated(List<Container> containers) {
        super.onContainersAllocated(containers);

        // pre sort
        List<Map.Entry<Resource, Queue<SettableFuture<Container>>>> new_allocated = this.resource_reqeusted.entrySet().stream().parallel()//
                .sorted((left, right) -> left.getKey().compareTo(right.getKey())) //
                .map(Function.identity()) //
                .collect(Collectors.toList());

        while (containers.size() > 0) {
            // try assign
            containers = containers.stream().parallel() //
                    .sorted((left, right) -> left.getResource().compareTo(right.getResource()))//
                    .filter((container) -> {
                        SettableFuture<Container> selected = new_allocated.parallelStream() //
                                .filter((entry) -> container.getResource().compareTo(entry.getKey()) >= 0) //
                                .findFirst() //
                                .map(Map.Entry::getValue) //
                                .orElse(new LinkedList<>()) //
                                .poll();

                        // notify
                        Optional<SettableFuture<Container>> optional = Optional.ofNullable(selected);
                        optional.orElse(SettableFuture.create()).set(container);

                        return !optional.isPresent();
                    })//
                    .collect(Collectors.toList());
        }
    }

    @Override
    public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
        super.onContainerStarted(containerId, allServiceResponse);

        this.container_launching.computeIfPresent(containerId, (key, old) -> {
            old.run();
            return null;
        });
    }
}

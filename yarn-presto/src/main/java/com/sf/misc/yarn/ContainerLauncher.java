package com.sf.misc.yarn;

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
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentMap;
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

    @Inject
    public ContainerLauncher(@ForOnYarn AMRMClientAsync master, @ForOnYarn NMClientAsync nodes, FileSystem hdfs, HttpServerInfo server_info) {
        this.master = master;
        this.nodes = nodes;
        this.hdfs = hdfs;
        this.server_info = server_info;

        this.resource_reqeusted = Maps.newConcurrentMap();
        this.container_released = Maps.newConcurrentMap();
        this.container_launching = Maps.newConcurrentMap();
    }

    public ListenableFuture<Container> launchContainer(ApplicationId appid, Class<?> entry_class, Resource resource) {
        // request contaienr
        SettableFuture<Container> reqeust = SettableFuture.create();
        this.resource_reqeusted.compute(resource, (key, old) -> {
            if (old == null) {
                old = Lists.newLinkedList();
            }

            old.add(reqeust);

            // request
            AMRMClient.ContainerRequest request = new AMRMClient.ContainerRequest(resource, null, null, Priority.UNDEFINED);
            this.master.addContainerRequest(request);
            return old;
        });

        ListenableFuture<ContainerLaunchContext> context_initializing = ExecutorServices.executor().submit(() -> createContext(appid, entry_class));

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

    protected ContainerLaunchContext createContext(ApplicationId appid, Class<?> entry_class) throws IOException {
        // ensure root
        Path workdir = new org.apache.hadoop.fs.Path("/tmp/unmanaged/" + appid);
        hdfs.mkdirs(workdir);

        // create jar
        Path jar_output = new Path(workdir, KICKSTART_JAR_FILE_NAME);
        try (OutputStream output = hdfs.create(jar_output);) {
            new JarCreator().add(KickStart.class).write(output);
        }

        // instruct update resoruces
        FileStatus status = hdfs.getFileStatus(jar_output);
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
        Map<String, String> enviroment = Maps.newTreeMap();

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
        commands.add(KickStart.class.getName());
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

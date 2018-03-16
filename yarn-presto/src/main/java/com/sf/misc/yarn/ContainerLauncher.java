package com.sf.misc.yarn;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import com.sf.misc.annotaions.ForOnYarn;
import com.sf.misc.async.ExecutorServices;
import io.airlift.log.Logger;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;

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

    private AMRMClientAsync master;
    private NMClientAsync nodes;
    private DFSClient hdfs;

    protected ConcurrentMap<Resource, Queue<SettableFuture<Container>>> resource_reqeusted;
    protected ConcurrentMap<ContainerId, SettableFuture<ContainerStatus>> container_releasing;
    protected ConcurrentMap<ContainerId, ExecutorServices.Lambda> container_launching;

    @Inject
    public ContainerLauncher(@ForOnYarn AMRMClientAsync master, @ForOnYarn NMClientAsync nodes) {
        this.master = master;
        this.nodes = nodes;
        this.resource_reqeusted = Maps.newConcurrentMap();
        this.container_releasing = Maps.newConcurrentMap();
        this.container_launching = Maps.newConcurrentMap();
    }

    public ListenableFuture<Container> requestContainer(Resource resource) {
        // set future
        SettableFuture<Container> future = SettableFuture.create();
        this.resource_reqeusted.compute(resource, (key, old) -> {
            if (old == null) {
                old = Lists.newLinkedList();
            }

            old.add(future);

            // request
            AMRMClient.ContainerRequest request = new AMRMClient.ContainerRequest(resource, null, null, Priority.UNDEFINED);
            this.master.addContainerRequest(request);
            return old;
        });

        return future;
    }

    public ListenableFuture<ContainerStatus> releaseContainer(Container container) {
        // set future
        SettableFuture<ContainerStatus> future = SettableFuture.create();
        this.container_releasing.putIfAbsent(container.getId(), future);

        // container may had been reqeusted but not launched,
        // so, be right with that situation.
        //TODO
        // release
        this.nodes.getContainerStatusAsync(container.getId(), container.getNodeId());
        //this.nodes.stopContainerAsync(container.getId(), container.getNodeId());

        return future;
    }

    public ListenableFuture<Container> launchContainer(Container container, ContainerLaunchContext context) {
        // set future
        SettableFuture<Container> future = SettableFuture.create();
        this.container_launching.putIfAbsent(container.getId(), () -> future.set(container));

        // start
        this.nodes.startContainerAsync(container, context);

        return future;
    }

    @Override
    public void onContainerStatusReceived(ContainerId container_id, ContainerStatus status) {
        this.container_releasing.computeIfPresent(container_id, (key, future) -> {
            switch (status.getState()) {
                case NEW:
                    this.master.releaseAssignedContainer(container_id);
                    break;
                case RUNNING:
                    //TODO
                    //this.nodes.stopContainerAsync(container_id, key.getContainerId());
                    break;
            }
            return null;
        });
    }

    @Override
    public void onContainersCompleted(List<ContainerStatus> statuses) {
        super.onContainersCompleted(statuses);

        statuses.stream().parallel().forEach((status) -> {
            this.container_releasing.computeIfPresent(status.getContainerId(), (key, future) -> {
                future.set(status);
                return null;
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

            // or it is released?
            //TODO
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

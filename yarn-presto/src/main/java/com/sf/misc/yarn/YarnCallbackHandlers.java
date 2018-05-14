package com.sf.misc.yarn;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.sf.misc.annotaions.ForOnYarn;
import io.airlift.log.Logger;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public class YarnCallbackHandlers extends YarnCallbackHandler implements Collection<YarnCallbackHandler> {


    public static final Logger LOGGER = Logger.get(YarnCallbackHandlers.class);

    protected ConcurrentMap<Class<? extends YarnCallbackHandler>, YarnCallbackHandler> handlers;

    public YarnCallbackHandlers() {
        this.handlers = Maps.newConcurrentMap();
    }

    @Override
    public void onContainersCompleted(List<ContainerStatus> statuses) {
        statuses.parallelStream().forEach((status) -> LOGGER.info("container complete:" + status));
        handlers.values().parallelStream().forEach((handler) -> handler.onContainersCompleted(statuses));
    }

    @Override
    public void onContainersAllocated(List<Container> containers) {
        containers.parallelStream().forEach((container) -> LOGGER.info("container allocated:" + container));
        handlers.values().parallelStream().forEach((handler) -> handler.onContainersAllocated(containers));
    }

    @Override
    public void onShutdownRequest() {
        LOGGER.info("application shutdown");
        handlers.values().parallelStream().forEach((handler) -> handler.onShutdownRequest());
    }

    @Override
    public void onNodesUpdated(List<NodeReport> updatedNodes) {
        updatedNodes.parallelStream().forEach((report) -> LOGGER.info("node updated:" + report));
        handlers.values().parallelStream().forEach((handler) -> handler.onNodesUpdated(updatedNodes));
    }

    @Override
    public float getProgress() {
        return handlers.values().parallelStream() //
                .map(YarnCallbackHandler::getProgress) //
                .filter(x -> x > 0) //
                .collect(Collectors.averagingDouble((x) -> x)) //
                .floatValue();
    }

    @Override
    public void onError(Throwable e) {
        LOGGER.error(e, "application unexpected error");
        handlers.values().parallelStream().forEach((handler) -> handler.onError(e));
    }

    @Override
    public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
        LOGGER.info("container started:" + containerId + " with context:["  //
                + allServiceResponse.keySet().parallelStream().collect(Collectors.joining(","))  //
                + "]");
        handlers.values().parallelStream().forEach((handler) -> handler.onContainerStarted(containerId, allServiceResponse));
    }

    @Override
    public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
        LOGGER.info("container updated:" + containerId + " status:" + containerStatus);
        handlers.values().parallelStream().forEach((handler) -> handler.onContainerStatusReceived(containerId, containerStatus));
    }

    @Override
    public void onContainerStopped(ContainerId containerId) {
        LOGGER.info("container stop:" + containerId);
        handlers.values().parallelStream().forEach((handler) -> handler.onContainerStopped(containerId));
    }

    @Override
    public void onStartContainerError(ContainerId containerId, Throwable t) {
        LOGGER.error(t, "container start fail:" + containerId);
        handlers.values().parallelStream().forEach((handler) -> handler.onStartContainerError(containerId, t));
    }

    @Override
    public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
        LOGGER.error(t, "container status fail:" + containerId);
        handlers.values().parallelStream().forEach((handler) -> handler.onGetContainerStatusError(containerId, t));
    }

    @Override
    public void onStopContainerError(ContainerId containerId, Throwable t) {
        LOGGER.error(t, "container stop fail:" + containerId);
        handlers.values().parallelStream().forEach((handler) -> handler.onStopContainerError(containerId, t));
    }

    @Override
    public int size() {
        return handlers.size();
    }

    @Override
    public boolean isEmpty() {
        return handlers.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return handlers.containsKey(o instanceof Class ? o : o.getClass());
    }

    @Override
    public Iterator<YarnCallbackHandler> iterator() {
        return handlers.values().iterator();
    }

    @Override
    public YarnCallbackHandler[] toArray() {
        return handlers.values().toArray(new YarnCallbackHandler[0]);
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return handlers.values().toArray(a);
    }

    @Override
    public boolean add(YarnCallbackHandler handler) {
        return handlers.putIfAbsent(handler.getClass(), handler) == null;
    }

    @Override
    public boolean remove(Object o) {
        return handlers.remove(o instanceof Class ? o : o.getClass()) != null;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return handlers.containsKey(c.parallelStream().map((x) -> x instanceof Class ? x : x.getClass()));
    }

    @Override
    public boolean addAll(Collection<? extends YarnCallbackHandler> c) {
        return c.parallelStream() //
                .map((x) -> this.add(x)) //
                .reduce((left, right) -> left && right) //
                .get();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return c.parallelStream() //
                .map((x) -> remove(x))
                .reduce((left, right) -> left && right) //
                .get();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        Set<Class> retain_types = c.parallelStream().map((x) -> x instanceof Class ? (Class) x : x.getClass()).collect(Collectors.toSet());
        return this.removeIf((handler) -> !retain_types.contains(handler.getClass()));
    }

    @Override
    public void clear() {
        this.handlers.clear();
    }
}

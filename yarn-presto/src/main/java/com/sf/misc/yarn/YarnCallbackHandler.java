package com.sf.misc.yarn;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class YarnCallbackHandler implements AMRMClientAsync.CallbackHandler, NMClientAsync.CallbackHandler {

    public static final Log LOGGER = LogFactory.getLog(YarnCallbackHandler.class);

    public YarnCallbackHandler() {
    }

    @Override
    public void onContainersCompleted(List<ContainerStatus> statuses) {
        statuses.parallelStream().forEach((status) -> LOGGER.info("container complete:" + status));
    }

    @Override
    public void onContainersAllocated(List<Container> containers) {
        containers.parallelStream().forEach((container) -> LOGGER.info("container allocated:" + container));
    }

    @Override
    public void onShutdownRequest() {
        LOGGER.info("application shutdown");
    }

    @Override
    public void onNodesUpdated(List<NodeReport> updatedNodes) {
        updatedNodes.parallelStream().forEach((report) -> LOGGER.info("node updated:" + report));
    }

    @Override
    public float getProgress() {
        return 0;
    }

    @Override
    public void onError(Throwable e) {
        LOGGER.error("application unexpected error", e);
    }

    @Override
    public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
        LOGGER.info("container started:" + containerId + " with context:["  //
                + allServiceResponse.keySet().parallelStream().collect(Collectors.joining(","))  //
                + "]");
    }

    @Override
    public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
        LOGGER.info("container updated:" + containerId + " status:" + containerStatus);
    }

    @Override
    public void onContainerStopped(ContainerId containerId) {
        LOGGER.info("container stop:" + containerId);
    }

    @Override
    public void onStartContainerError(ContainerId containerId, Throwable t) {
        LOGGER.error("container fail start:" + containerId, t);
    }

    @Override
    public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
        LOGGER.error("container fail status:" + containerId, t);
    }

    @Override
    public void onStopContainerError(ContainerId containerId, Throwable t) {
        LOGGER.error("container fail stop:" + containerId, t);
    }
}

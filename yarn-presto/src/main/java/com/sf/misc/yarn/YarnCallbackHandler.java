package com.sf.misc.yarn;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class YarnCallbackHandler implements AMRMClientAsync.CallbackHandler, NMClientAsync.CallbackHandler {

    @Override
    public void onContainersCompleted(List<ContainerStatus> statuses) {
    }

    @Override
    public void onContainersAllocated(List<Container> containers) {
    }

    @Override
    public void onShutdownRequest() {
    }

    @Override
    public void onNodesUpdated(List<NodeReport> updatedNodes) {
    }

    @Override
    public float getProgress() {
        return 0;
    }

    @Override
    public void onError(Throwable e) {
    }

    @Override
    public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
    }

    @Override
    public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
    }

    @Override
    public void onContainerStopped(ContainerId containerId) {
    }

    @Override
    public void onStartContainerError(ContainerId containerId, Throwable t) {
    }

    @Override
    public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
    }

    @Override
    public void onStopContainerError(ContainerId containerId, Throwable t) {
    }
}

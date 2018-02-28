package com.sf.misc.yarn;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class Kickstart {

    public static final Log LOGGER = LogFactory.getLog(Kickstart.class);

    public static void main(String args[]) {
        UserGroupInformation.createRemoteUser("you know what?").doAs(new PrivilegedAction<Object>() {
            @Override
            public Object run() {
                proxy();
                return null;
            }
        });
    }

    public static void proxy() {
        List<Optional<Runnable>> defers = new LinkedList<>();
        ListeningExecutorService pool = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
        Object stop = new Object();
        try {
            Configuration configuration = new Configuration();
            configuration.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
            configuration.set(YarnConfiguration.RM_HA_IDS, "rm1,rm2");
            configuration.set(YarnConfiguration.RM_HOSTNAME + ".rm1", "10.202.77.200");
            configuration.set(YarnConfiguration.RM_HOSTNAME + ".rm2", "10.202.77.201");

            YarnClient client = YarnClient.createYarnClient();
            client.init(configuration);
            client.start();
            defers.add(Optional.of(client::stop));

            client.getApplications(new TreeSet<>(Arrays.asList("YARN", "unmanaged")),
                    EnumSet.of(YarnApplicationState.ACCEPTED, //
                            YarnApplicationState.NEW, //
                            YarnApplicationState.NEW_SAVING, //
                            YarnApplicationState.RUNNING, //
                            YarnApplicationState.SUBMITTED //
                    )).stream().forEach((application -> {
                try {
                    client.killApplication(application.getApplicationId());
                } catch (Exception e) {
                    LOGGER.error("fail kill application:" + application, e);
                }
            }));

            YarnClientApplication application = client.createApplication();

            // request new appid,attach to resource manager
            GetNewApplicationResponse response = application.getNewApplicationResponse();
            ApplicationId app_id = response.getApplicationId();
            LOGGER.info("application id:" + app_id);
            LOGGER.info("resources:" + response.getMaximumResourceCapability());

            // prepare context
            ApplicationSubmissionContext context = application.getApplicationSubmissionContext();
            context.setApplicationName("just a test");
            context.setApplicationId(app_id);
            context.setApplicationType("unmanaged");

            // set appmaster launch spec
            context.setAMContainerSpec(ContainerLaunchContext.newInstance(null, null, null, null, null, null));

            // unmanaged app master
            context.setUnmanagedAM(true);

            LOGGER.info("submit application:" + client.submitApplication(context));

            // setup token
            // this is necessary for unmanaged application
            Token<AMRMTokenIdentifier> token = client.getAMRMToken(app_id);
            UserGroupInformation.getCurrentUser().addToken(token);

            // connect app master to nodeservice
            NMClientAsync nodes = NMClientAsync.createNMClientAsync(new NMClientAsync.CallbackHandler() {
                @Override
                public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
                    LOGGER.info("started container:" + containerId);
                }

                @Override
                public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
                    LOGGER.info("containers statsus:" + containerStatus);
                }

                @Override
                public void onContainerStopped(ContainerId containerId) {
                    LOGGER.info("container stoped");
                    synchronized (stop) {
                        stop.notifyAll();
                    }
                }

                @Override
                public void onStartContainerError(ContainerId containerId, Throwable t) {
                    LOGGER.info("start container fail");
                }

                @Override
                public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
                    LOGGER.info("fail to get contaienr status");
                }

                @Override
                public void onStopContainerError(ContainerId containerId, Throwable t) {
                    LOGGER.info("stop container fail");
                }
            });
            nodes.init(configuration);
            nodes.start();
            defers.add(Optional.of(nodes::stop));

            // connect app master to resroucemanager
            AMRMClientAsync master = AMRMClientAsync.createAMRMClientAsync(1000, new AMRMClientAsync.CallbackHandler() {
                @Override
                public void onContainersCompleted(List<ContainerStatus> statuses) {
                }

                @Override
                public void onContainersAllocated(List<Container> containers) {
                    containers.stream().forEach((container) -> {
                        LOGGER.info("allocated contaienr:" + container);
                        ContainerLaunchContext launch_context = ContainerLaunchContext.newInstance(null, null, null, null, null, null);
                        nodes.startContainerAsync(container, launch_context);
                    });
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
            });
            master.init(configuration);
            master.start();
            defers.add(Optional.of(master::stop));

            // register appmaster
            master.registerApplicationMaster(InetAddress.getLocalHost().getHostName(), 0, null);
            defers.add(Optional.of(() -> {
                try {
                    master.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "api stoped", null);
                } catch (Throwable throwable) {
                    LOGGER.error("fail to unregister appmaster", throwable);
                }
            }));

            // then request container
            AMRMClient.ContainerRequest container_reqeust = new AMRMClient.ContainerRequest( //
                    Resource.newInstance(128, 1), //
                    null, null, //
                    Priority.UNDEFINED);
            master.addContainerRequest(container_reqeust);

            synchronized (stop) {
                stop.wait();
            }
        } catch (Exception e) {
            LOGGER.error("unexpcetd exception", e);
        } finally {
            defers.stream().forEach((defer) -> defer.orElse(() -> {
            }).run());
            LOGGER.info("done");
        }
    }
}

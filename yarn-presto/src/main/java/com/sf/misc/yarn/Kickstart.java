package com.sf.misc.yarn;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RpcEngine;
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
import org.apache.hadoop.yarn.factories.RpcServerFactory;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
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

            client.getApplications(new TreeSet<>(Arrays.asList("YARN", "unmanaged")),
                    EnumSet.of(YarnApplicationState.ACCEPTED, //
                            YarnApplicationState.NEW, //
                            YarnApplicationState.NEW_SAVING, //
                            YarnApplicationState.RUNNING, //
                            YarnApplicationState.SUBMITTED //
                    )).parallelStream().forEach((application -> {
                try {
                    client.killApplication(application.getApplicationId());
                } catch (Exception e) {
                    LOGGER.error("fail kill application:" + application, e);
                }
            }));

            ApplicationBuilder application = new ApplicationBuilder(configuration);
            CountDownLatch latch = new CountDownLatch(1);
            YarnCallbackHandler handler = newHandler(application, latch);

            // connect app master to nodeservice
            NMClientAsync nodes = NMClientAsync.createNMClientAsync(handler);

            // connect app master to resroucemanager
            AMRMClientAsync master = AMRMClientAsync.createAMRMClientAsync(1000, handler);

            // build application
            application.rpc(new InetSocketAddress(8080)) //
                    .trackWith(null) //
                    .withName("just a test") //
                    .withYarnClient(client) //
                    .withAMRMClient(master) //
                    .withNMClient(nodes) //
                    .whenUp(() -> {
                        AMRMClient.ContainerRequest request = new AMRMClient.ContainerRequest(Resource.newInstance(128, 1), null, null, Priority.UNDEFINED);
                        application.getMaster().addContainerRequest(request);

                        LOGGER.info("request a container:" + request);
                    })
                    .build() //
                    .get();

            // awai termination
            latch.await();
            client.killApplication(application.getApplication().getNewApplicationResponse().getApplicationId());

            LOGGER.info("stop application");
            application.stop().get();
        } catch (Exception e) {
            LOGGER.error("unexpcetd exception", e);
        } finally {
            LOGGER.info("done");
        }
    }

    protected static YarnCallbackHandler newHandler(ApplicationBuilder application, CountDownLatch latch) {
        return new YarnCallbackHandler() {
            public void onContainersAllocated(List<Container> containers) {
                super.onContainersAllocated(containers);
                containers.parallelStream().forEach(container -> {
                    ContainerLaunchContext context = ContainerLaunchContext.newInstance(null, null, null, null, null, null);
                    application.getNodes().startContainerAsync(container, context);
                });
            }
        };
    }
}

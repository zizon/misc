package com.sf.misc.presto;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.sf.misc.airlift.Airlift;
import com.sf.misc.async.ExecutorServices;
import com.sf.misc.classloaders.HttpClassLoaderModule;
import com.sf.misc.yarn.ContainerAssurance;
import com.sf.misc.yarn.EchoResource;
import com.sf.misc.yarn.YarnApplication;
import com.sf.misc.yarn.YarnApplicationModule;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceInventory;
import io.airlift.discovery.client.ServiceState;
import io.airlift.log.Logger;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.xml.ws.Service;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.sql.Time;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;

public class TestPrestoContainer {
    public static final Logger LOGGER = Logger.get(TestPrestoContainer.class);

    Airlift airlift;
    String app_name = "yarn-presto-container";

    @Before
    public void setupClient() throws Exception {
        Map<String, String> configuration = new HashMap<>();

        configuration.put("node.environment", "yarn");
        configuration.put("discovery.uri", "http://" + InetAddress.getLocalHost().getHostAddress() + ":8080");
        //configuration.put("service-inventory.uri", configuration.get("discovery.uri") + "/v1/service");
        configuration.put("discovery.store-cache-ttl", "0s");
        configuration.put("yarn.rms", "10.202.77.200,10.202.77.201");
        configuration.put("yarn.hdfs", "test-cluster://10.202.77.200:8020,10.202.77.201:8020");

        airlift = new Airlift().withConfiguration(configuration) //
                .module(new YarnApplicationModule()) //
                .module(new HttpClassLoaderModule()) //
                .module(new PrestoContainerModule()) //
                .module(new Module() {
                    @Override
                    public void configure(Binder binder) {
                        jaxrsBinder(binder).bind(EchoResource.class);
                        //binder.bind(HttpClient.class).to(JettyHttpClient.class).in(Scopes.SINGLETON);
                        //httpServerBinder(binder).bindResource("/", "webapp").withWelcomeFile("index.html");
                    }
                })
                .start();
    }

    @After
    public void cleanup() throws Exception {
        YarnClient client = airlift.getInstance(YarnApplication.class).getYarn();
        if (client != null) {
            client.getApplications(new TreeSet<>(Arrays.asList("YARN", "unmanaged")),
                    EnumSet.of(YarnApplicationState.ACCEPTED, //
                            YarnApplicationState.NEW, //
                            YarnApplicationState.NEW_SAVING, //
                            YarnApplicationState.RUNNING, //
                            YarnApplicationState.SUBMITTED //
                    )).parallelStream() //
                    .filter((app) -> app.getName().equals(app_name)) //
                    .forEach((application -> {
                        try {
                            client.killApplication(application.getApplicationId());
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }));
        }
    }

    protected void killOld(ApplicationId self) throws Exception {
        YarnClient client = airlift.getInstance(YarnApplication.class).getYarn();
        client.getApplications(new TreeSet<>(Arrays.asList("YARN", "unmanaged")),
                EnumSet.of(YarnApplicationState.ACCEPTED, //
                        YarnApplicationState.NEW, //
                        YarnApplicationState.NEW_SAVING, //
                        YarnApplicationState.RUNNING, //
                        YarnApplicationState.SUBMITTED //
                )).parallelStream() //
                .filter((app) -> app.getName().equals(app_name) && app.getApplicationId().compareTo(self) != 0) //
                .forEach((application -> {
                    try {
                        client.killApplication(application.getApplicationId());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }));
    }

    @Test
    public void test() throws Exception {
        YarnApplication application = airlift.getInstance(YarnApplication.class) //
                .runas("yarn") //
                .withName(app_name) //
                .httpListenAt(new InetSocketAddress(80)) //
                .build().get();

        ApplicationReport report = application.getYarn().getApplicationReport(application.getApplication().getNewApplicationResponse().getApplicationId());
        killOld(report.getApplicationId());

        //System.out.println(properties);
        //Assert.fail();

        // set up launcer
        PrestoContainerLauncher launcher = airlift.getInstance(PrestoContainerLauncher.class);
        ContainerAssurance assurance = airlift.getInstance(ContainerAssurance.class);

        ListenableFuture<Boolean> coodinator = assurance.secure("coordinator", () -> launcher.launchContainer(report.getApplicationId(), true), 1);
        ListenableFuture<Boolean> workers = assurance.secure("worker", () -> launcher.launchContainer(report.getApplicationId(), false), 1);


        LOGGER.info("coordinator" + Futures.getUnchecked(coodinator) + "\n" //
                + "worker:" + Futures.getUnchecked(workers) //
        );

        LockSupport.park();
        /*
        ListenableFuture<Container> coordinator = launcher.launchContainer(report.getApplicationId(), true);
        ListenableFuture<Container> worker = launcher.launchContainer(report.getApplicationId(), false);

        for(;;) {
            ListenableFuture<ContainerStatus> status = Futures.transformAsync(coordinator, (container) -> {
                return launcher.launcher().containerStatus(container);
            });

            LOGGER.info("status:" + status.get());
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
        }
        */
        /*
        for (; ; ) {
            LOGGER.info("-------------------");
            ServiceInventory serviceInventory = airlift.getInstance(ServiceInventory.class);
            Iterable<ServiceDescriptor> discovery = serviceInventory.getServiceDescriptors();
            for (ServiceDescriptor descriptor:discovery) {
                LOGGER.info(descriptor.toString());
            }

            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(5));
        }
        */
        //application.stop();

        //LockSupport.park();
    }
}

package com.sf.misc.presto;

import com.facebook.presto.client.Column;
import com.facebook.presto.hive.HiveStorageFormat;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import com.sf.misc.airlift.Airlift;
import com.sf.misc.annotaions.ForOnYarn;
import com.sf.misc.classloaders.HttpClassLoaderModule;
import com.sf.misc.yarn.ContainerAssurance;
import com.sf.misc.yarn.EchoResource;
import com.sf.misc.yarn.YarnApplication;
import com.sf.misc.yarn.YarnApplicationModule;
import com.sf.misc.yarn.YarnRMProtocol;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceInventory;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.log.Logger;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.InetAddress;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;

public class TestPrestoContainer {
    public static final Logger LOGGER = Logger.get(TestPrestoContainer.class);

    Airlift airlift;

    @Before
    public void setupClient() throws Exception {
        Map<String, String> configuration = new HashMap<>();

        configuration.put("node.environment", "yarn");
        configuration.put("http-server.http.port", "8080");
        configuration.put("discovery.uri", "http://" + InetAddress.getLocalHost().getHostAddress() + ":" + configuration.get("http-server.http.port"));
        configuration.put("service-inventory.uri", configuration.get("discovery.uri") + "/v1/service");
        configuration.put("discovery.store-cache-ttl", "0s");

        configuration.put("yarn.rms", "10.202.77.200,10.202.77.201");
        configuration.put("hdfs.nameservices", "test-cluster://10.202.77.200:8020,10.202.77.201:8020");
        configuration.put("yarn.rpc.user.proxy", "anyone");
        configuration.put("yarn.rpc.user.real", "hive");
        configuration.put("yarn.application.name", "yarn-presto");

        configuration.put("hive.metastore.uri", "thrift://10.202.77.200:9083");

        configuration.put("ranger.policy.name", "hivedev");
        configuration.put("ranger.admin.url", "http://10.202.77.200:6080");
        configuration.put("ranger.audit.solr.url", "http://10.202.77.200:6083/solr/ranger_audits");
        configuration.put("ranger.audit.solr.collection", "ranger_audits");
        //configuration.put("log.enable-console","false");
        //configuration.put("log.path","./presto.log");

        configuration.put("log.levels-file",  //
                new File(Thread.currentThread().getContextClassLoader() //
                        .getResource("airlift-log.properties") //
                        .toURI() //
                ).getAbsolutePath() //
        );

        HiveStorageFormat.class.getName();

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
        YarnRMProtocol client = airlift.getInstance(Key.get(YarnRMProtocol.class, ForOnYarn.class));
        client.getApplications(GetApplicationsRequest.newInstance(new TreeSet<>(Arrays.asList("YARN", YarnApplication.APPLICATION_TYPE)),
                EnumSet.of(YarnApplicationState.ACCEPTED, //
                        YarnApplicationState.NEW, //
                        YarnApplicationState.NEW_SAVING, //
                        YarnApplicationState.RUNNING, //
                        YarnApplicationState.SUBMITTED //
                ))).getApplicationList().parallelStream().forEach((application -> {
            try {
                client.forceKillApplication(KillApplicationRequest.newInstance(application.getApplicationId()));
            } catch (Exception e) {
                //e.printStackTrace();
            }
        }));
    }

    public void killOld(ApplicationId self) throws Exception {
        YarnRMProtocol client = airlift.getInstance(Key.get(YarnRMProtocol.class, ForOnYarn.class));
        client.getApplications(GetApplicationsRequest.newInstance(new TreeSet<>(Arrays.asList("YARN", YarnApplication.APPLICATION_TYPE)),
                EnumSet.of(YarnApplicationState.ACCEPTED, //
                        YarnApplicationState.NEW, //
                        YarnApplicationState.NEW_SAVING, //
                        YarnApplicationState.RUNNING, //
                        YarnApplicationState.SUBMITTED //
                ))).getApplicationList().parallelStream().filter((applicaiton) -> {
            return applicaiton.getApplicationId().compareTo(self) != 0;
        }).forEach((application -> {
            try {
                client.forceKillApplication(KillApplicationRequest.newInstance(application.getApplicationId()));
            } catch (Exception e) {
                //e.printStackTrace();
            }
        }));
    }

    @Test
    public void test() throws Exception {
        URI server_address = airlift.getInstance(Key.get(HttpServerInfo.class)).getHttpExternalUri();
        YarnApplication application = airlift.getInstance(Key.get(YarnApplication.class));
        YarnRMProtocol client = airlift.getInstance(Key.get(YarnRMProtocol.class, ForOnYarn.class));
        ApplicationReport report = client.getApplicationReport(GetApplicationReportRequest.newInstance(Futures.getUnchecked(application.getApplication()))).getApplicationReport();
        killOld(report.getApplicationId());

        //System.out.println(properties);
        //Assert.fail();

        // set up launcer
        PrestoContainerLauncher launcher = airlift.getInstance(Key.get(PrestoContainerLauncher.class));
        ContainerAssurance assurance = airlift.getInstance(Key.get(ContainerAssurance.class));
        ServiceInventory inventory = airlift.getInstance(Key.get(ServiceInventory.class));

        ListenableFuture<Container> coodinator = launcher.launchContainer(true, Optional.empty());
        ListenableFuture<Container> workers = launcher.launchContainer(false, Optional.empty());
        LOGGER.info("coordinator:" + Futures.getUnchecked(coodinator)  //
                + " worker:" + Futures.getUnchecked(workers) //
        );

        LOGGER.info("wait for coordinator...");
        for (; ; ) {
            inventory.updateServiceInventory();
            Iterable<ServiceDescriptor> iterable = inventory.getServiceDescriptors("presto-coordinator");
            if (iterable.iterator().hasNext()) {
                break;
            }
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(5));
        }

        LOGGER.info("presto cluster ok");
        for (ServiceDescriptor descriptor : inventory.getServiceDescriptors("presto-coordinator")) {
            URL server = new URL(descriptor.getProperties().get("http"));
            LOGGER.info("connecting :" + server);

            SessionBuilder.PrestoSession session = new SessionBuilder() //
                    .coordinator(server.toURI()) //
                    .doAs("hive") //
                    .token("hello") //
                    .build();
            String query = "select * from test limit 100";
            Iterator<List<Map.Entry<Column, Object>>> iterator = session.query(query, (stat) -> {
                LOGGER.info(stat.toString());
            }).get();

            iterator.forEachRemaining((row) -> {
                LOGGER.info("row: " + row.stream().map((entry) -> {
                    LOGGER.info(entry.getKey().getTypeSignature().toString());
                    LOGGER.info(entry.getValue().getClass().toString());
                    return entry.getKey().toString() + ":" + entry.getValue().toString();
                }).collect(Collectors.joining(",")));
            });

            LOGGER.info("done a query:" + query);
            break;
        }

        //LockSupport.park();
    }
}

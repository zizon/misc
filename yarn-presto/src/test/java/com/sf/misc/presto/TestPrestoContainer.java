package com.sf.misc.presto;

import com.facebook.presto.client.ClientSession;
import com.facebook.presto.client.Column;
import com.facebook.presto.client.QueryData;
import com.facebook.presto.client.StatementClient;
import com.facebook.presto.client.StatementClientFactory;
import com.facebook.presto.hive.HiveStorageFormat;
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
import io.airlift.http.server.HttpServer;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import okhttp3.OkHttpClient;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

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
        configuration.put("service-inventory.uri", configuration.get("discovery.uri") + "/v1/service");
        configuration.put("discovery.store-cache-ttl", "0s");
        configuration.put("yarn.rms", "10.202.77.200,10.202.77.201");
        configuration.put("yarn.hdfs", "test-cluster://10.202.77.200:8020,10.202.77.201:8020");
        configuration.put("hdfs.nameservices", configuration.get("yarn.hdfs"));
        configuration.put("hive.metastore.uri", "thrift://10.202.77.200:9083");
        configuration.put("yarn.container.minimun-resource", "1GB");
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
        URI server_address = airlift.getInstance(HttpServerInfo.class).getHttpExternalUri();
        YarnApplication application = airlift.getInstance(YarnApplication.class) //
                .runas("anyone") //
                .withName(app_name) //
                .trackWith(server_address)
                .build().get();

        ApplicationReport report = application.getYarn().getApplicationReport(application.getApplication().getNewApplicationResponse().getApplicationId());
        killOld(report.getApplicationId());

        //System.out.println(properties);
        //Assert.fail();

        // set up launcer
        PrestoContainerLauncher launcher = airlift.getInstance(PrestoContainerLauncher.class);
        ContainerAssurance assurance = airlift.getInstance(ContainerAssurance.class);
        ServiceInventory inventory = airlift.getInstance(ServiceInventory.class);

        for (; ; ) {
            inventory.updateServiceInventory();
            Iterable<ServiceDescriptor> iterable = inventory.getServiceDescriptors("presto-coordinator");
            if (iterable.iterator().hasNext()) {
                break;
            }

            LOGGER.info("wait for coordinator...");
            ListenableFuture<Boolean> coodinator = assurance.secure("coordinator", () -> launcher.launchContainer(report.getApplicationId(), true, Optional.empty()), 1);
            ListenableFuture<Boolean> workers = assurance.secure("worker", () -> launcher.launchContainer(report.getApplicationId(), false, Optional.empty()), 1);

            LOGGER.info("coordinator:" + Futures.getUnchecked(coodinator)  //
                    + " worker:" + Futures.getUnchecked(workers) //
            );

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

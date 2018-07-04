package com.sf.misc.yarn;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.sf.misc.airlift.Airlift;
import com.sf.misc.annotaions.ForOnYarn;
import com.sf.misc.async.ExecutorServices;
import com.sf.misc.async.FutureExecutor;
import com.sf.misc.classloaders.HttpClassLoaderModule;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StaticBodyGenerator;
import io.airlift.http.client.StringResponseHandler;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.log.Logger;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.Path;
import java.net.InetAddress;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;

public class TestYarnApplication {

    public static final Logger LOGGER = Logger.get(TestYarnApplication.class);

    Airlift airlift;

    @Before
    public void setupClient() throws Exception {
        Map<String, String> configuration = new HashMap<>();

        configuration.put("node.environment", "test");
        configuration.put("discovery.uri", "http://" + InetAddress.getLocalHost().getHostName() + ":8080");
        configuration.put("discovery.store-cache-ttl", "0s");
        configuration.put("yarn.rms", "10.202.77.200,10.202.77.201");
        configuration.put("hdfs.nameservices", "test-cluster://10.202.77.200:8020,10.202.77.201:8020");
        configuration.put("yarn.rpc.user.real", "hive");
        configuration.put("yarn.rpc.user.proxy", "anyone");

        airlift = new Airlift().withConfiguration(configuration) //
                .module(new YarnApplicationModule()) //
                .module(new HttpClassLoaderModule()) //
                .module(new Module() {
                    @Override
                    public void configure(Binder binder) {
                        jaxrsBinder(binder).bind(EchoResource.class);
                        binder.bind(HttpClient.class).to(JettyHttpClient.class).in(Scopes.SINGLETON);
                    }
                })
                .start();
    }

    @After
    public void cleanup() throws Exception {
        YarnRMProtocol client = airlift.getInstance(Key.get(YarnRMProtocol.class, ForOnYarn.class));
        client.getApplications(GetApplicationsRequest.newInstance(new TreeSet<>(Arrays.asList("YARN", "unmanaged")),
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

    @Test
    public void test() throws Exception {
        URI server_address = airlift.getInstance(Key.get(HttpServerInfo.class)).getHttpExternalUri();
        YarnApplication application = airlift.getInstance(Key.get(YarnApplication.class));
        YarnRMProtocol yarn = airlift.getInstance(Key.get(YarnRMProtocol.class, ForOnYarn.class));
        ApplicationReport report = yarn.getApplicationReport(GetApplicationReportRequest.newInstance(Futures.getUnchecked(application.getApplication()))).getApplicationReport();

        // test echo
        String body = "hello kitty";
        URI http = airlift.getInstance(Key.get(HttpServerInfo.class)).getHttpUri();
        URL request = new URL(http.toURL(), EchoResource.class.getAnnotation(Path.class).value());

        HttpClient client = airlift.getInstance(Key.get(HttpClient.class));
        StringResponseHandler.StringResponse response = client.execute(Request.builder() //
                        .setMethod("POST") //
                        .setBodyGenerator(StaticBodyGenerator.createStaticBodyGenerator(body.getBytes())) //
                        .setUri(request.toURI()).build(),
                StringResponseHandler.createStringResponseHandler());
        Assert.assertEquals(body, response.getBody());

        ContainerLauncher launcher = airlift.getInstance(Key.get(ContainerLauncher.class));
        ListenableFuture<Container> launched = launcher.launchContainer( //
                TestYarnApplication.class, //
                Resource.newInstance(128, 1) //
        );

        launched.get();

        ContainerId status = Futures.getUnchecked(FutureExecutor.transformAsync(launched, (container) -> {
            Assert.assertNotNull(container);
            LOGGER.info("try release:" + container);
            return launcher.releaseContainer(container);
        }));
        Assert.assertNotNull(status);
    }

    public static void main(String[] args) {
        System.out.println("it works");
    }
}

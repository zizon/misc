package com.sf.misc.yarn;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.sf.misc.airlift.Airlift;
import com.sf.misc.classloaders.HttpClassLoaderModule;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StaticBodyGenerator;
import io.airlift.http.client.StringResponseHandler;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.log.Logger;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.Container;
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
import java.net.InetSocketAddress;
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
        configuration.put("yarn.hdfs", "test-cluster://10.202.77.200:8020,10.202.77.201:8020");

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
        YarnClient client = airlift.getInstance(YarnApplication.class).getYarn();
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
                //e.printStackTrace();
            }
        }));
    }


    @Test
    public void test() throws Exception {
        YarnApplication application = airlift.getInstance(YarnApplication.class) //
                .runas("yarn") //
                .withName("just a test") //
                .httpListenAt(new InetSocketAddress(80)) //
                .build().get();

        ApplicationReport report = application.getYarn().getApplicationReport(application.getApplication().getNewApplicationResponse().getApplicationId());
        Assert.assertNotNull(report);
        Assert.assertEquals(FinalApplicationStatus.UNDEFINED, report.getFinalApplicationStatus());

        ContainerLauncher launcher = airlift.getInstance(ContainerLauncher.class);
        Assert.assertNotNull(airlift.getInstance(ContainerLauncher.class));

        YarnCallbackHandlers handlers = airlift.getInstance(YarnCallbackHandlers.class);
        Assert.assertNotNull(handlers);

        int size = handlers.size();
        Assert.assertTrue(handlers.add(launcher));
        Assert.assertFalse(handlers.add(launcher));
        Assert.assertEquals(1, handlers.size() - size);

        Assert.assertTrue(handlers.remove(launcher));
        Assert.assertEquals(0, handlers.size() - size);
        Assert.assertFalse(handlers.remove(launcher));

        handlers.add(launcher);
        Assert.assertTrue(handlers.remove(launcher.getClass()));

        // add back
        handlers.add(launcher);

        // test echo
        String body = "hello kitty";
        URI http = airlift.getInstance(HttpServerInfo.class).getHttpUri();
        URL request = new URL(http.toURL(), EchoResource.class.getAnnotation(Path.class).value());

        HttpClient client = airlift.getInstance(HttpClient.class);
        StringResponseHandler.StringResponse response = client.execute(Request.builder() //
                        .setMethod("POST") //
                        .setBodyGenerator(StaticBodyGenerator.createStaticBodyGenerator(body.getBytes())) //
                        .setUri(request.toURI()).build(),
                StringResponseHandler.createStringResponseHandler());
        Assert.assertEquals(body, response.getBody());

        ListenableFuture<Container> launched = launcher.launchContainer( //
                report.getApplicationId(), //
                TestYarnApplication.class, //
                Resource.newInstance(128, 1) //
        );

        launched.get();

        ContainerStatus status = Futures.transformAsync(launched, (container) -> {
            Assert.assertNotNull(container);
            LOGGER.info("try release:" + container);
            return launcher.releaseContainer(container);
        }).get(60, TimeUnit.SECONDS);
        Assert.assertNotNull(status);
    }

    public static void main(String[] args) {
        System.out.println("it works");
    }
}

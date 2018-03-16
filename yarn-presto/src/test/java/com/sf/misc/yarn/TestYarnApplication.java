package com.sf.misc.yarn;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.sf.misc.airlift.Airlift;
import com.sf.misc.async.ExecutorServices;
import com.sf.misc.classloaders.HttpClassLoaderModule;
import com.sf.misc.classloaders.HttpClassloaderResource;
import com.sf.misc.classloaders.JarCreator;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StaticBodyGenerator;
import io.airlift.http.client.StringResponseHandler;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.log.Logger;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.Path;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
        //configuration.put("yarn.client.polling","1s");

        //URI uri = URI.create("test-cluster://10.202.77.200/test");
        //System.out.println(uri.getScheme());
        //System.exit(0);

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

        // test request/release container
        IntStream.range(0, 1).parallel() //
                .mapToObj((ignore) -> {
                            Resource resource = Resource.newInstance(128, 1);
                            return Futures.transformAsync(launcher.requestContainer(resource), (container) -> {
                                Assert.assertTrue(container.getResource().compareTo(resource) >= 0);
                                return launcher.releaseContainer(container);
                            });
                        } //
                ) //
                .map(future -> (ExecutorServices.Lambda) (() -> future.get(30, TimeUnit.SECONDS))) //
                .forEach(ExecutorServices.Lambda::run);

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

        // test launcer container
        ListenableFuture<Container> started = Futures.transformAsync(launcher.requestContainer(Resource.newInstance(128, 1)), (container) -> {
            FileSystem hdfs = airlift.getInstance(FileSystem.class);
            org.apache.hadoop.fs.Path workdir = new org.apache.hadoop.fs.Path("/tmp/unmanaged/" + application.getApplication().getNewApplicationResponse().getApplicationId());
            hdfs.mkdirs(workdir);
            org.apache.hadoop.fs.Path jar_output = new org.apache.hadoop.fs.Path(workdir, "kickstart.jar");
            try (OutputStream output = hdfs.create(jar_output);) {
                new JarCreator().add(KickStart.class).write(output);
            }
            FileStatus status = hdfs.getFileStatus(jar_output);

            Map<String, LocalResource> local_resoruce = Maps.newTreeMap();
            local_resoruce.put("kickstart.jar", LocalResource.newInstance(
                    ConverterUtils.getYarnUrlFromPath(status.getPath()), //
                    LocalResourceType.FILE, //
                    LocalResourceVisibility.APPLICATION,//
                    status.getLen(),//
                    status.getModificationTime() //
                    )
            );

            Map<String, String> enviroment = Maps.newTreeMap();
            enviroment.put(KickStart.HTTP_CLASSLOADER_URL,
                    new URL(airlift.getInstance(HttpServerInfo.class).getHttpUri().toURL(), HttpClassloaderResource.class.getAnnotation(Path.class).value() + "/").toExternalForm()
            );
            enviroment.put(KickStart.KICKSTART_CLASS, TestYarnApplication.class.getName());
            enviroment.put(ApplicationConstants.Environment.CLASSPATH.key(), ".:./*");

            List<String> commands = Lists.newLinkedList();
            //commands.add("cp -r -l . /tmp/" + application.getApplication().getNewApplicationResponse().getApplicationId() + ";");
            //commands.add("chmod 777 -R /tmp/" + application.getApplication().getNewApplicationResponse().getApplicationId() + ";");
            commands.add(ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java");
            commands.add(KickStart.class.getName());
            commands.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/" + ApplicationConstants.STDOUT);
            commands.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/" + ApplicationConstants.STDERR);

            ContainerLaunchContext context = ContainerLaunchContext.newInstance(local_resoruce, enviroment, commands, null, null, null);
            return launcher.launchContainer(container, context);
        });

        ContainerStatus status = Futures.transformAsync(started, (container) -> {
            LOGGER.info("container started");
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

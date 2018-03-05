package com.sf.misc.classloaders;

import com.google.common.collect.Maps;
import com.sf.misc.airlift.Airlift;
import com.sf.misc.async.Graph;
import com.sf.misc.async.TestGraph;
import io.airlift.discovery.client.DiscoveryLookupClient;
import io.airlift.discovery.client.ServiceDescriptors;
import org.junit.Assert;
import org.junit.Test;
import sun.reflect.Reflection;

import javax.ws.rs.Path;
import java.net.InetAddress;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;

public class TestHttpClassloaderResource {


    @Test
    public void test() {
        try {
            Map<String, String> config = Maps.newTreeMap();
            config.put("node.environment", "test");

            config.put("discovery.uri", "http://" + InetAddress.getLocalHost().getHostName() + ":8080");
            config.put("discovery.store-cache-ttl", "0s");


            Airlift airlift = new Airlift().withConfiguration(config);

            // start
            airlift.module(new HttpClassLoaderModule()).start();

            // reigster

            ServiceDescriptors service = airlift.getInstance(DiscoveryLookupClient.class).getServices(HttpClassLoaderModule.SERVICE_TYPE).get();

            Assert.assertNotNull(service);
            Assert.assertEquals(1, service.getServiceDescriptors().size());


            URL url = new URL(config.get("discovery.uri") //
                    + HttpClassloaderResource.class.getAnnotation(Path.class).value() + "/");

            String graph = Graph.class.getName();
            String test_graph = TestGraph.class.getName();

            Assert.assertNotNull(Class.forName(graph));
            Assert.assertNotNull(Class.forName(test_graph));

            //http://SF0001369351A:8080/v1/http-classloader/com/sf/misc/async/Graph.class

            //LockSupport.park();
            // try load class
            HttpClassloader classloader = new HttpClassloader(new URL[]{url}, null).use();
            Class.forName(graph);
            Class.forName(test_graph);
            classloader.release();

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

    }

}

package com.sf.misc.classloaders;

import com.sf.misc.async.Promises;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.zip.ZipFile;

public class TestClassResolver {

    @Test
    public void testLoadClass() throws Exception {
        Arrays.asList(
                //AMRMClientAsync.CallbackHandler.class,
                Class.forName("com/google/common/collect/ImmutableList$1".replace("/", ".")), //
                Class.forName("org/apache/bval/BeanValidationContext$1".replace("/", ".")) //
                //Class.forName("org/apache/bval/cdi/BValExtension".replace("/", "."))
        ).stream().forEach((clazz) -> {
            ((Promises.PromiseRunnable) () -> {
                URL url = ClassResolver.locate(clazz).get();
                System.out.println(url);
                url.openConnection().getInputStream();
                Assert.assertNotNull(url);
            }).run();
        });
    }

    @Test
    public void testLoadResrouces() throws Exception {
        String jmx = "META-INF/services/javax.management.remote.JMXConnectorServerProvider";
        String resource = "META-INF/services/javax.validation.spi.ValidationProvider";
        String services = "META-INF/services";
        ClassLoader loader = Thread.currentThread().getContextClassLoader();

        URL located = loader.getResource(resource);
        Assert.assertNotNull(loader);
        System.out.println(located);

        Enumeration<URL> urls = loader.getResources(services);
        while (urls.hasMoreElements()) {
            System.out.println("---------------");
            URL url = urls.nextElement();
            if (url.getProtocol() != "jar") {
                continue;
            }

            String full = url.toExternalForm().substring("jar:".length());
            String file = full.substring(0, full.indexOf("!"));
            System.out.println(file);
            ZipFile zip = new ZipFile(new File(new URI(file)));

            zip.stream() //
                    .filter((entry) -> entry.getName().startsWith("META-INF/services")) //
                    .forEach((entry) -> {
                        System.out.println(entry);
                    });
        }
    }

    @Test
    public void testWebappResources() throws Exception {
        System.out.println(ClassResolver.resource("webapp"));
        ClassResolver.resource("webapp/v1/memory");
    }

    @Test
    public void testClassURL() throws Exception{
        String class_name = "com.facebook.presto.hive.HdfsEnvironment";
        URL url = Thread.currentThread().getContextClassLoader().getResource(class_name.replace(".","/") + ".class");
        System.out.println(url);
    }
}

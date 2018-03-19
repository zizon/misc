package com.sf.misc.classloaders;

import com.sf.misc.async.ExecutorServices;
import org.apache.bval.cdi.BValExtension;
import org.junit.Assert;
import org.junit.Test;

import java.net.URL;
import java.util.Arrays;
import java.util.Enumeration;

public class TestClassResolver {

    @Test
    public void testLoadClass() throws Exception {
        Arrays.asList(
                //AMRMClientAsync.CallbackHandler.class,
                Class.forName("com/google/common/collect/ImmutableList$1".replace("/", ".")), //
                Class.forName("org/apache/bval/BeanValidationContext$1".replace("/", ".")), //
                Class.forName("org/apache/bval/cdi/BValExtension".replace("/", "."))
        ).stream().forEach((clazz) -> {
            ((ExecutorServices.Lambda) () -> {
                URL url = ClassResolver.locate(clazz).get();
                System.out.println(url);
                url.openConnection().getInputStream();
                Assert.assertNotNull(url);
            }).run();
        });
    }

    @Test
    public void testLoadResrouces() throws Exception {
        String resource = "META-INF/services/javax.validation.spi.ValidationProvider";
        ClassLoader loader = Thread.currentThread().getContextClassLoader();

        URL located = loader.getResource(resource);
        Assert.assertNotNull(loader);
        System.out.println(located);

        Enumeration<URL> urls = loader.getResources(resource);
        while (urls.hasMoreElements()) {
            System.out.println(urls.nextElement());
        }
    }
}

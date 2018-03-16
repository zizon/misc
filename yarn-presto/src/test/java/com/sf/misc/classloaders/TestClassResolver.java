package com.sf.misc.classloaders;

import com.sf.misc.async.ExecutorServices;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class TestClassResolver {

    @Test
    public void test() throws Exception {

        Arrays.asList(
                //AMRMClientAsync.CallbackHandler.class,
                Class.forName("com/google/common/collect/ImmutableList$1".replace("/", "."))
        ).stream().forEach((clazz) -> {
            ((ExecutorServices.Lambda) () -> {
                URL url = ClassResolver.locate(clazz).get();
                System.out.println(url);
                url.openConnection().getInputStream();
                Assert.assertNotNull(url);
            }).run();
        });
    }
}

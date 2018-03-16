package com.sf.misc.classloaders;

import com.sf.misc.yarn.KickStart;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import sun.tools.jar.resources.jar;

import java.io.File;
import java.io.FileOutputStream;
import java.util.jar.JarFile;

public class TestJarCreator {
    File test;

    @Before
    public void remove() {
        test = new File("test.jar");
        if (test.exists()) {
            test.delete();
        }
    }

    @After
    public void cleanup() {
        test.delete();
    }


    @Test
    public void test() throws Exception {
        JarCreator creator = new JarCreator();

        FileOutputStream output = new FileOutputStream(test);

        creator.add(KickStart.class);

        creator.write(output);

        try (JarFile jar = new JarFile(test);) {
            Assert.assertEquals(1, jar.stream().count());
        }
    }
}

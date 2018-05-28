package com.sf.misc.classloaders;

import com.sf.misc.yarn.KickStart;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import sun.tools.jar.resources.jar;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.jar.Attributes;
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
        creator.manifest(Attributes.Name.CLASS_PATH.toString(), ".:./");

        creator.write(output);

        try (JarFile jar = new JarFile(test);) {
            Assert.assertEquals(2, jar.stream().count());
            jar.stream().forEach((entry) -> {
                try {
                    System.out.println(entry.getName());
                    System.out.println(entry.getAttributes());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
    }
}

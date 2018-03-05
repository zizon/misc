package com.sf.misc.classloaders;

import org.junit.Assert;
import org.junit.Test;

import java.net.URL;

public class TestHttpClassloader {
    @Test
    public void test() {
        Exception exception = null;
        try (HttpClassloader classloader_a = new HttpClassloader(
                new URL[]{
                        Thread.currentThread().getContextClassLoader().getResource("")
                }, //
                Thread.currentThread().getContextClassLoader() //
        ); //
             HttpClassloader classloader_b = new HttpClassloader(
                     new URL[]{
                             Thread.currentThread().getContextClassLoader().getResource("")
                     }, //
                     Thread.currentThread().getContextClassLoader() //
             );
        ) {
            // use a
            classloader_a.use();
            Assert.assertTrue(classloader_a.inContext());
            Assert.assertFalse(classloader_b.inContext());

            // use a,b
            classloader_b.use();
            Assert.assertTrue(classloader_a.inContext());
            Assert.assertTrue(classloader_b.inContext());

            // use a,b,b
            classloader_b.use();
            Assert.assertTrue(classloader_a.inContext());
            Assert.assertTrue(classloader_b.inContext());

            // use b,b
            classloader_a.release();
            Assert.assertFalse(classloader_a.inContext());
            Assert.assertTrue(classloader_b.inContext());

            try {
                // assert not using a
                classloader_a.release();
                Assert.fail();
            } catch (Exception e) {
            } finally {
                Assert.assertFalse(classloader_a.inContext());
                Assert.assertTrue(classloader_b.inContext());
            }

            // use b
            classloader_b.release();
            Assert.assertFalse(classloader_a.inContext());
            Assert.assertTrue(classloader_b.inContext());

            // use none
            classloader_b.release();
            Assert.assertFalse(classloader_a.inContext());
            Assert.assertFalse(classloader_b.inContext());
        } catch (Exception e) {
            exception = e;
        } finally {
            Assert.assertNotNull(exception);
        }
    }
}

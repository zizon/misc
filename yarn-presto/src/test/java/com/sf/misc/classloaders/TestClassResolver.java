package com.sf.misc.classloaders;

import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.junit.Assert;
import org.junit.Test;

public class TestClassResolver {

    @Test
    public void test() {
        Class<?> clazz = AMRMClientAsync.CallbackHandler.class;
        //System.out.println( clazz.getResource(""));
        System.out.println(ClassResolver.locate(clazz).get());
        Assert.assertNotNull(ClassResolver.locate(clazz).get());
    }
}

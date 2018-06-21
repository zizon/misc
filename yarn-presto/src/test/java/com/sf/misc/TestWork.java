package com.sf.misc;

import org.apache.hadoop.security.GroupMappingServiceProvider;
import org.apache.hadoop.security.JniBasedUnixGroupsMappingWithFallback;
import org.junit.Assert;
import org.junit.Test;

public class TestWork {

    @Test
    public void test() throws Exception{
        Assert.assertTrue(GroupMappingServiceProvider.class.isAssignableFrom(JniBasedUnixGroupsMappingWithFallback.class));
    }

}

package com.sf.misc.ranger;

import org.apache.hadoop.security.GroupMappingServiceProvider;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class TestRanger {

    @Test
    public void test() throws Exception {
        RangerPolicy policy = new RangerPolicy.RangerPolicyBuilder() //
                .policy("hivedev") //
                .admin("http://test:6080") //
                .solr("http://test:6083/solr/ranger_audits") //
                .collection("ranger_audits") //
                .build();

        boolean allow = policy.newAccess("nfsnobody")
                .privilege("create").accessDatabase("sf_bdp") //
                .access().privilege("write").accessDatabase("default") //
                .access().isAccessAllowed().get();

        Assert.assertTrue(allow);

        Assert.assertTrue(GroupMappingServiceProvider.class.isAssignableFrom(RangerGroupMapping.class));
        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(10));
    }
}

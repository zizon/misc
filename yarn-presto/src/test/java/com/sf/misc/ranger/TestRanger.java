package com.sf.misc.ranger;

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

        policy.newAccess("nfsnobody")
                .privilege("create").accessDatabase("sf_bdp") //
                .access().privilege("write").accessDatabase("default") //
                .access().isAccessAllowed();

        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(10));
    }
}

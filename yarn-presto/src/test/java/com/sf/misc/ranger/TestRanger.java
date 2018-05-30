package com.sf.misc.ranger;

import org.apache.ranger.admin.client.RangerAdminRESTClient;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineImpl;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineOptions;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.junit.Test;

import java.util.Collections;
import java.util.Date;

public class TestRanger {

    @Test
    public void test() throws Exception {
        String service_name = "hivedev";
        String property_prefix = "ranger.plugin.hive";

        RangerConfiguration configuration = RangerConfiguration.getInstance();
        configuration.set(property_prefix + ".policy.rest.url", "http://test:6080");

        RangerAdminRESTClient admin = new RangerAdminRESTClient();
        admin.init(service_name, "a test client", property_prefix);

        ServicePolicies policies = admin.getServicePoliciesIfUpdated(-1, System.currentTimeMillis());
        policies.getPolicies().forEach((policy) -> {
            System.out.println("------------------");
            System.out.println(policy);
        });

        RangerPolicyEngine engine = new RangerPolicyEngineImpl("a test engine", policies, new RangerPolicyEngineOptions());
        //System.out.println(engine);
        RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        resource.setValue("database","sf_bdp");
        resource.setValue("table","*");
        resource.setValue("column","*");

        RangerAccessRequestImpl request = new RangerAccessRequestImpl();
        request.setUser("nfsnobody");
        //request.setUserGroups(Collections.emptySet());
        request.setResource(resource);
        //request.setAccessTime(new Date());
        //request.setAction("query");
        request.setAccessType("create");

        engine.preProcess(request);
        System.out.println(engine.isAccessAllowed(request, null));
    }
}

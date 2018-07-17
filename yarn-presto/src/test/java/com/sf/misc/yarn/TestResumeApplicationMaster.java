package com.sf.misc.yarn;

import com.google.common.util.concurrent.SettableFuture;
import com.sf.misc.airlift.Airlift;
import com.sf.misc.async.Promises;
import io.airlift.log.Logger;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.AMRMTokenSecretManagerState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.AMRMTokenSecretManagerStatePBImpl;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.security.Key;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;

public class TestResumeApplicationMaster {

    public static final Logger LOGGER = Logger.get(TestResumeApplicationMaster.class);

    @Test
    public void doAS() throws Exception {
        UserGroupInformation.createProxyUser("anyone", UserGroupInformation.createRemoteUser("hive")).doAs(new PrivilegedExceptionAction<Object>() {
            @Override
            public Object run() throws Exception {
                test();
                return null;
            }
        });
    }

    public void test() throws Exception {
        Configuration configuration = new Configuration();
        ConfigurationGenerator generator = new ConfigurationGenerator();

        // resource managers
        generator.generateYarnConfiguration("10.202.77.200,10.202.77.201").entrySet() //
                .forEach((entry) -> {
                    configuration.set(entry.getKey(), entry.getValue());
                });

        configuration.set(YarnConfiguration.RM_ZK_ADDRESS, "10.202.77.200:2181,10.202.77.201:2181,10.202.77.202:2181");
        configuration.set(YarnConfiguration.ZK_RM_STATE_STORE_PARENT_PATH, "/rmstore");

        String amrm_token_path = configuration.get(YarnConfiguration.ZK_RM_STATE_STORE_PARENT_PATH) + "/ZKRMStateRoot/AMRMTokenSecretManagerRoot";
        CuratorFramework zookeeper = CuratorFrameworkFactory.builder() //
                .canBeReadOnly(true) //
                .retryPolicy(new BoundedExponentialBackoffRetry(10, 5000, 10))
                .connectString(configuration.get(YarnConfiguration.RM_ZK_ADDRESS)) //
                .build();
        zookeeper.start();
        LOGGER.info("start zookeeeper");

        YarnClient client = YarnClient.createYarnClient();
        client.init(configuration);
        client.start();

        AMRMClient master = AMRMClient.createAMRMClient();
        master.init(configuration);
        master.start();

        ApplicationId id = ApplicationId.newInstance(1529482095838l, 5429);
        ApplicationReport report = client.getApplicationReport(id);

        SettableFuture<org.apache.hadoop.security.token.Token<AMRMTokenIdentifier>> token = SettableFuture.create();
        zookeeper.getData() //
                .inBackground( //
                        (ignore, event) -> {
                            LOGGER.info("got some:" + event);
                            KeeperException.Code code = KeeperException.Code.get(event.getResultCode());
                            switch (KeeperException.Code.get(event.getResultCode())) {
                                case OK:
                                    AMRMTokenSecretManagerStatePBImpl token_protoc = new AMRMTokenSecretManagerStatePBImpl(YarnServerResourceManagerRecoveryProtos.AMRMTokenSecretManagerStateProto.parseFrom(event.getData()));
                                    AMRMTokenSecretManagerState token_state = AMRMTokenSecretManagerState.newInstance(token_protoc.getCurrentMasterKey(), token_protoc.getNextMasterKey());
                                    AMRMTokenIdentifier identifier =
                                            new AMRMTokenIdentifier(report.getCurrentApplicationAttemptId(), token_state.getCurrentMasterKey()
                                                    .getKeyId());

                                    Mac mac = Mac.getInstance("HmacSHA1");
                                    ByteBuffer secret_key = token_state.getCurrentMasterKey().getBytes();
                                    Key key = new SecretKeySpec(secret_key.array(), "HmacSHA1");
                                    mac.init(key);

                                    byte[] password = mac.doFinal(identifier.getBytes());
                                    token.set(new Token<AMRMTokenIdentifier>(identifier.getBytes(), password,
                                            identifier.getKind(), new Text()));
                                    break;
                                default:
                                    token.setException(KeeperException.create(code));
                                    break;
                            }
                        },  //
                        Promises.executor() //
                ).forPath(amrm_token_path);

        LOGGER.info("try add token");
        UserGroupInformation.getCurrentUser().addToken(token.get());

        LOGGER.info("register master");
        //System.out.println(client.getApplicationReport(id).getAMRMToken());
        master.unregisterApplicationMaster(report.getFinalApplicationStatus(), "steal master", "");
        //RegisterApplicationMasterResponse response = master.registerApplicationMaster("10.202.106.158",50924,"http://10.202.106.158:50924");
        //master.unregisterApplicationMaster(report.getFinalApplicationStatus(),"steal master","");
        //LOGGER.info("response:" + response);
        master.addContainerRequest(new AMRMClient.ContainerRequest(Resource.newInstance(1024, 1), null, null, Priority.UNDEFINED));
        LOGGER.info("response:" + master.allocate(0.1f));
    }

    @Test
    public void test3() throws Throwable {
        Map<String, String> configuration = new HashMap<>();

        configuration.put("node.environment", "yarn");
        configuration.put("http-server.http.port", "8080");
        configuration.put("discovery.uri", "http://" + InetAddress.getLocalHost().getHostAddress() + ":" + configuration.get("http-server.http.port"));
        configuration.put("service-inventory.uri", configuration.get("discovery.uri") + "/v1/service");
        configuration.put("discovery.store-cache-ttl", "0s");

/*
        new Airlift().withConfiguration(configuration) //
                .start();
        LOGGER.info("one");

        configuration.put("http-server.http.port", "8081");
        new Airlift().withConfiguration(configuration) //
                .start();
        LOGGER.info("twor");
        */
    }

}

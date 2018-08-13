package com.sf.misc.hadoop.sasl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdfs.security.token.block.BlockPoolTokenSecretManager;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtocolInfo;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.WritableRpcEngine;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ServiceLoader;
import java.util.concurrent.locks.LockSupport;

public class TestSasl {

    public static final Log LOGGER = LogFactory.getLog(TestSasl.class);

    @ProtocolInfo(protocolName = "SaslTest", protocolVersion = 2)
    public static interface Protocol {
        public void test();
    }

    public static class ProtocolServer implements Protocol {

        @Override
        public void test() {
            System.out.println("ok");
            LOGGER.error("invoke ok");
            LOGGER.info("info invoke ok");
        }
    }

    @Test
    public void test() throws IOException {
        ServiceLoader.load(SecurityInfo.class).forEach((info)->{
            LOGGER.info(info);
        });
        //System.exit(0);
        WritableRpcEngine.ensureInitialized();

        Configuration configuration = new Configuration();
        configuration.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION, UserGroupInformation.AuthenticationMethod.TOKEN.name());

        byte[] identifier = "test_id".getBytes();
        byte[] password = "a password".getBytes();
        Text kind = new Text("test token kind");
        Text service = new Text("test service");

        UserGroupInformation.getCurrentUser().addToken(new Token(identifier, password, kind, service));
        int port = 80;
        Server server = new RPC.Builder(configuration)
                .setPort(port)
                .setVerbose(true)
                .setInstance(new ProtocolServer())
                .setProtocol(Protocol.class)
                .setSecretManager(new BlockPoolTokenSecretManager())
                .build();

        Protocol client = RPC.getProxy(Protocol.class, 1, new InetSocketAddress(80), configuration);
        server.start();

        client.test();
        LockSupport.park();
    }
}

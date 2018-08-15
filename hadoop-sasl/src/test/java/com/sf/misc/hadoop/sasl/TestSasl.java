package com.sf.misc.hadoop.sasl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtocolInfo;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.WritableRpcEngine;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ServiceLoader;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class TestSasl {

    public static final Log LOGGER = LogFactory.getLog(TestSasl.class);

    protected static final String TOKEN_ENV = "_token_file_";
    protected static final URI TOKEN_FILE = new File(TOKEN_ENV).toURI();

    private static final long DELEGATION_TOKEN_REMOVER_SCAN_INTERVAL =
            TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS);

    protected static final byte[] PASSWORD = "a password".getBytes();

    @ProtocolInfo(protocolName = "SaslTest", protocolVersion = 2)
    public static interface Protocol {
        public void test();
    }

    public static class ProtocolServer implements Protocol {

        @Override
        public void test() {
            LOGGER.info("info invoke ok");
        }
    }

    @Before
    public void setupToken() throws Throwable {
        Text service = new Text("test service");
        SaslTokenIdentifier raw_token = new SaslTokenIdentifier();
        raw_token.sign();
        Token<SaslTokenIdentifier> token = new Token(raw_token.getBytes(), PASSWORD, raw_token.getKind(), service);

        try {
            UserGroupInformation.getLoginUser();
        } catch (Throwable e) {
        }
        Credentials credentials = new Credentials();
        credentials.addToken(token.getService(), token);
        try (DataOutputStream stream = new DataOutputStream(new FileOutputStream(new File(TOKEN_FILE)))) {
            credentials.writeTokenStorageToStream(stream);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        try (DataInputStream stream = new DataInputStream(new FileInputStream(new File(TOKEN_FILE)))) {
            LOGGER.info("recover token");
            credentials = new Credentials();
            credentials.readTokenStorageStream(stream);
            UserGroupInformation.getCurrentUser().addCredentials(credentials);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    public void test() throws IOException {
        ServiceLoader.load(SecurityInfo.class).forEach((info) -> {
            LOGGER.info("seciryty info:" + info);
        });

        //System.exit(0);
        WritableRpcEngine.ensureInitialized();

        Configuration configuration = new Configuration();
        configuration.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION, UserGroupInformation.AuthenticationMethod.TOKEN.name());

        EmptyTokenIdentifier empty_token = new EmptyTokenIdentifier();
        //UserGroupInformation.getCurrentUser().addToken(new Token(raw_token.getBytes(), password, raw_token.getKind(), service));
        //UserGroupInformation.getCurrentUser().addToken(new Token(empty_token.getBytes(), "".getBytes(), empty_token.getKind(), new Text("protocol service")));
        UserGroupInformation.getCurrentUser().getCredentials().getAllTokens().forEach((token) -> {
            LOGGER.info("use token:" + token);
        });
        int port = 80;

        Configuration conf = configuration;
        Server server = new RPC.Builder(configuration)
                .setPort(port)
                .setVerbose(true)
                .setInstance(new ProtocolServer())
                .setProtocol(Protocol.class)
                .setSecretManager(new SecretManager() {
                    @Override
                    protected byte[] createPassword(TokenIdentifier identifier) {
                        return "".getBytes();
                    }

                    @Override
                    public byte[] retrievePassword(TokenIdentifier identifier) throws InvalidToken {
                        return "".getBytes();
                    }

                    @Override
                    public TokenIdentifier createIdentifier() {
                        return new EmptyTokenIdentifier();
                    }
                })
                .build();

        Protocol client = RPC.getProxy(Protocol.class, 1, new InetSocketAddress(80), configuration);
        server.start();

        client.test();
    }
}

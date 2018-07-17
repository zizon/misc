package com.sf.misc.airlift;

import io.airlift.discovery.client.DiscoveryClientConfig;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.log.Logger;
import org.junit.Test;

import java.net.URI;
import java.util.concurrent.locks.LockSupport;

public class TestAirlift {

    public static final Logger LOGGER = Logger.get(TestAirlift.class);

    @Test
    public void test() {
        AirliftConfig config = new AirliftConfig();
        config.setNodeEnv("test");

        Airlift airlift = new Airlift(config).start().unchecked();


        LockSupport.park();
    }
}

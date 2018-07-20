package com.sf.misc.airlift;

import com.sf.misc.yarn.YarnRediscoveryModule;
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
        config.setPort(8080);
        config.setNodeEnv("test");

        Airlift airlift = new Airlift(config) //
                .module(YarnRediscoveryModule.createNew("hello")) //
                .start().unchecked();

        LockSupport.park();
    }
}

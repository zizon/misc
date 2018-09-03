package com.sf.misc.airlift;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.sf.misc.airlift.federation.Federation;
import com.sf.misc.airlift.federation.ServiceSelectors;
import com.sf.misc.async.Promises;
import com.sf.misc.classloaders.HttpClassLoaderModule;
import com.sf.misc.yarn.rediscovery.YarnRediscovery;
import com.sf.misc.yarn.rediscovery.YarnRediscoveryModule;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.DiscoveryLookupClient;
import io.airlift.discovery.client.ForDiscoveryClient;
import io.airlift.discovery.client.HttpDiscoveryLookupClient;
import io.airlift.discovery.client.ServiceDescriptorsRepresentation;
import io.airlift.http.client.HttpClient;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import org.junit.Test;

import javax.annotation.PostConstruct;
import java.io.File;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class TestAirlift {

    public static final Logger LOGGER = Logger.get(TestAirlift.class);

    public static class TestRdiscovery {
        protected final LoadingCache<URI, DiscoveryLookupClient> discovery_lookup;

        @Inject
        public TestRdiscovery(
                NodeInfo node,
                JsonCodec<ServiceDescriptorsRepresentation> serviceDescriptorsCodec,
                @ForDiscoveryClient
                        HttpClient http
        ) {


            this.discovery_lookup = CacheBuilder.newBuilder().build(new CacheLoader<URI, DiscoveryLookupClient>() {
                @Override
                public DiscoveryLookupClient load(URI key) throws Exception {
                    return new HttpDiscoveryLookupClient(() -> key, node, serviceDescriptorsCodec, http);
                }
            });
        }

        @PostConstruct
        public void start() {
            URI uri = URI.create("http://localhost:8080");
            Promises.schedule(() -> {
                LOGGER.info("try get client");
                DiscoveryLookupClient client = discovery_lookup.getUnchecked(uri);
                LOGGER.info("client :" + client);
            }, TimeUnit.SECONDS.toMillis(5), true).logException();

        }
    }

    protected void startMaster() throws Throwable {
        AirliftConfig config = new AirliftConfig();
        config.setPort(8080);
        config.setNodeEnv("test");
        config.setLoglevelFile(new File(Thread.currentThread().getContextClassLoader().getResource("airlift-log.properties").toURI()).getAbsolutePath());

        new Airlift(config) //\
                .module(new HttpClassLoaderModule()) //
                .module(new YarnRediscoveryModule("hello"))
                .module(new Module() {
                    @Override
                    public void configure(Binder binder) {
                        binder.bind(TestRdiscovery.class).in(Scopes.SINGLETON);
                    }
                })
                .start();
    }


    @Test
    public void test() throws Throwable {
        startMaster();

        LockSupport.park();
    }
}

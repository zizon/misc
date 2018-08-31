package com.sf.misc.airlift.federation;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Inject;
import com.sf.misc.airlift.DependOnDiscoveryService;
import com.sf.misc.async.ListenablePromise;
import com.sf.misc.async.Promises;
import io.airlift.discovery.client.DiscoveryClientConfig;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceInventory;
import io.airlift.log.Logger;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.URLConnection;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.time.temporal.TemporalUnit;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class DiscoveryUpdator {

    public static final Logger LOGGER = Logger.get(DiscoveryUpdator.class);

    protected final ServiceSelectors selectors;
    protected final DiscoveryClientConfig config;
    protected final AtomicReference<List<ServiceDescriptor>> serviceDescriptors;

    @Inject
    public DiscoveryUpdator(ServiceSelectors selectors,
                            DiscoveryClientConfig config,
                            ServiceInventory inventory) throws NoSuchFieldException, IllegalAccessException, IOException {
        this.selectors = selectors;
        this.config = config;

        // save modifier
        Field inventory_field = inventory.getClass().getDeclaredField("serviceDescriptors");
        inventory_field.setAccessible(true);
        serviceDescriptors = (AtomicReference<List<ServiceDescriptor>>) inventory_field.get(inventory);
    }

    @PostConstruct
    public void start() {
        Promises.schedule(
                this::updateDiscoveryURI,
                TimeUnit.SECONDS.toMillis(5),
                true
        ).logException();
    }

    protected void updateDiscoveryURI() {
        // do not use service selector,as it use cache data
        // that will fail annoucement if the selected discoery server is down.
        // use federation and discovery lookup instead.
        selectors.selectServiceForType(DependOnDiscoveryService.DISCOVERY_SERVICE_TYPE).parallelStream()
                .map((service) -> {
                    URI discovery_uri = Federation.http(service);
                    LOGGER.debug("usable discovery uri:" + discovery_uri);
                    return discovery_uri;
                })
                .sorted()
                .findFirst()
                .ifPresent((uri) -> {
                    config.setDiscoveryServiceURI(uri);
                    LOGGER.debug("update discovery uri to:" + uri);

                    // update inventory
                    updateInventory();
                });
    }

    protected void updateInventory() {
        serviceDescriptors.set(selectors.selectServiceForType(Federation.DISCOVERY_SERVICE_TYPE));
    }
}


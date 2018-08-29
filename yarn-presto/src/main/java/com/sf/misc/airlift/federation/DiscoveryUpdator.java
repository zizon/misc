package com.sf.misc.airlift.federation;

import com.google.inject.Inject;
import com.sf.misc.airlift.DependOnDiscoveryService;
import com.sf.misc.async.Promises;
import io.airlift.discovery.client.DiscoveryClientConfig;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceInventory;
import io.airlift.log.Logger;

import javax.annotation.PostConstruct;
import javax.ws.rs.Path;
import java.lang.reflect.Field;
import java.net.URI;
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
                            ServiceInventory inventory) throws NoSuchFieldException, IllegalAccessException {
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
        selectors.selectServiceForType(DependOnDiscoveryService.DISCOVERY_SERVICE_TYPE).parallelStream()
                .map((service) -> {
                    URI discovery_uri = Federation.http(service);
                    LOGGER.debug("usable discovery uri:" + discovery_uri);
                    return discovery_uri;
                })
                .sorted()
                .findAny()
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


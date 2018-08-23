package com.sf.misc.airlift.federation;

import com.google.inject.Inject;
import com.sf.misc.airlift.DependOnDiscoveryService;
import com.sf.misc.async.Promises;
import io.airlift.discovery.client.DiscoveryClientConfig;
import io.airlift.log.Logger;

import javax.annotation.PostConstruct;
import javax.swing.text.html.Option;
import java.net.URI;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class DiscoveryUpdator {

    public static final Logger LOGGER = Logger.get(DiscoveryUpdator.class);

    protected final ServiceSelectors selectors;
    protected final DiscoveryClientConfig config;

    @Inject
    public DiscoveryUpdator(ServiceSelectors selectors,
                            DiscoveryClientConfig config) {
        this.selectors = selectors;
        this.config = config;
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
        Optional<URI> uri = selectors.selectServiceForType(DependOnDiscoveryService.DISCOVERY_SERVICE_TYPE).parallelStream()
                .map((service) -> {
                    URI discovery_uri = Federation.http(service);
                    LOGGER.debug("usable discovery uri:" + discovery_uri);
                    return discovery_uri;
                })
                .findAny();

        if (uri.isPresent()) {
            config.setDiscoveryServiceURI(uri.get());
        }

        LOGGER.debug("update discovery uri to:" + config.getDiscoveryServiceURI());
    }
}


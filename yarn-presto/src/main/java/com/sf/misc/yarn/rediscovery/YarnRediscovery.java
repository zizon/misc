package com.sf.misc.yarn.rediscovery;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;
import com.sf.misc.airlift.DependOnDiscoveryService;
import com.sf.misc.airlift.federation.Federation;
import com.sf.misc.airlift.federation.FederationModule;
import com.sf.misc.airlift.federation.ServiceSelectors;
import com.sf.misc.async.Promises;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.DiscoveryLookupClient;
import io.airlift.discovery.client.ForDiscoveryClient;
import io.airlift.discovery.client.HttpDiscoveryLookupClient;
import io.airlift.discovery.client.ServiceAnnouncement;
import io.airlift.discovery.client.ServiceDescriptorsRepresentation;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.discovery.client.ServiceSelectorFactory;
import io.airlift.http.client.HttpClient;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;

import javax.annotation.PostConstruct;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.net.URI;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class YarnRediscovery extends DependOnDiscoveryService {

    public static final Logger LOGGER = Logger.get(YarnRediscovery.class);

    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface ForYarnRediscovery {
    }

    protected static final String SERVICE_TYPE = "yarn-rediscovery";
    protected static final String GROUP_PROPERTY = "group";

    protected final LoadingCache<URI, DiscoveryLookupClient> discovery_lookup;
    protected final ServiceSelectors selectors;
    protected final Federation federation;
    protected final NodeInfo node;
    protected final String group;

    @Inject
    public YarnRediscovery(Announcer announcer,
                           NodeInfo node,
                           JsonCodec<ServiceDescriptorsRepresentation> serviceDescriptorsCodec,
                           @ForDiscoveryClient HttpClient http,
                           Federation federation,
                           @ForYarnRediscovery String group,
                           HttpServerInfo httpServerInfo,
                           ServiceSelectors selectors
    ) {
        super(SERVICE_TYPE, announcer, httpServerInfo, ImmutableMap.of(GROUP_PROPERTY, group));
        this.federation = federation;
        this.node = node;
        this.group = group;
        this.selectors = selectors;

        this.discovery_lookup = CacheBuilder.newBuilder().build(new CacheLoader<URI, DiscoveryLookupClient>() {
            @Override
            public DiscoveryLookupClient load(URI key) throws Exception {
                return new HttpDiscoveryLookupClient(() -> key, node, serviceDescriptorsCodec, http);
            }
        });
    }

    @PostConstruct
    public void start() {
        this.activate();

        LOGGER.debug("start resiscovery...");
        // skip if not fedration provider
        if (!discoveryEnabled()) {
            LOGGER.debug("resiscovery not enableed");
            return;
        }

        Promises.schedule(//
                this::rediscovery, //
                TimeUnit.SECONDS.toMillis(5), //
                true
        ).logException();
    }


    protected void rediscovery() {
        LOGGER.debug("scheudle one rediscovery...");
        // find native discovery
        Set<URI> discovery_services = selectors.selectServiceForType(DISCOVERY_SERVICE_TYPE).parallelStream()
                .map(DependOnDiscoveryService::http)
                .distinct()
                .collect(Collectors.toSet());

        LOGGER.debug("find discovery service:" + discovery_services);

        // annouce to one that not in discovery but under a same group id
        selectors.selectServiceForType(Federation.SERVICE_TYPE).parallelStream() //
                // exclude self
                .filter((service) -> !service.getNodeId().equals(node))
                // collect all discovery server
                .map(DependOnDiscoveryService::http) //
                .distinct()
                // filter native discovery
                .filter((discovery) -> !discovery_services.contains(discovery))
                .forEach((uri) -> {
                    LOGGER.debug("touching fedration:" + uri);
                    Promises.submit(() -> discovery_lookup.getUnchecked(uri))
                            // fetch service
                            .transformAsync((client) -> client.getServices(SERVICE_TYPE))
                            // find rediscovery service
                            .callback((services, throwable) -> {
                                LOGGER.debug("uri:" + uri + " of services:" + services);
                                if (throwable != null) {
                                    LOGGER.error(throwable, "fail to access discovery service:" + uri);
                                    return;
                                }

                                // find service with same application id
                                services.getServiceDescriptors().parallelStream() //
                                        .map((service) -> {
                                            LOGGER.debug("from uri:" + uri + " service:" + service);
                                            return service;
                                        })
                                        // ensure type
                                        .filter((service) -> service.getType().equals(SERVICE_TYPE))
                                        // exclude self
                                        .filter((service) -> !service.getNodeId().equals(node))
                                        // find same node group
                                        .filter((service) -> group.equals(service.getProperties().get(GROUP_PROPERTY)))
                                        // to uri
                                        .map((service) -> {
                                            return URI.create(service.getProperties().get(Federation.HTTP_URI_PROPERTY));
                                        })
                                        .forEach((discovery_uri) -> {
                                            LOGGER.debug("annoucing to discovery:" + discovery_uri);
                                            federation.annouce(discovery_uri, announcer().getServiceAnnouncements());
                                        });
                            });
                });
    }
}

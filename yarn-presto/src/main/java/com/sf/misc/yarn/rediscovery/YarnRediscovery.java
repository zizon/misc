package com.sf.misc.yarn.rediscovery;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import com.sf.misc.airlift.federation.Federation;
import com.sf.misc.airlift.federation.FederationAnnouncer;
import com.sf.misc.async.Promises;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.DiscoveryLookupClient;
import io.airlift.discovery.client.ForDiscoveryClient;
import io.airlift.discovery.client.HttpDiscoveryLookupClient;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceDescriptorsRepresentation;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.discovery.client.ServiceSelectorConfig;
import io.airlift.discovery.client.ServiceSelectorFactory;
import io.airlift.http.client.HttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;

import java.net.URI;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class YarnRediscovery {

    public static final Logger LOGGER = Logger.get(YarnRediscovery.class);

    public static final String SERVICE_TYPE = "yarn-rediscovery";
    public static final String APPLICATION_PROPERTY = "application";

    protected static String DISCOVERY_SERVICE_TYPE = "discovery";

    protected final LoadingCache<URI, DiscoveryLookupClient> discovery_lookup;
    protected final NodeInfo node;
    protected final Announcer host_announcer;
    protected final FederationAnnouncer federation_announcer;
    protected final ServiceSelector federation;
    protected final ServiceSelector discovery;

    @Inject
    public YarnRediscovery(Announcer announcer,
                           FederationAnnouncer federation_announcer,
                           NodeInfo node,
                           JsonCodec<ServiceDescriptorsRepresentation> serviceDescriptorsCodec,
                           @ForDiscoveryClient HttpClient http,
                           ServiceSelectorFactory factory
    ) {
        this.federation_announcer = federation_announcer;
        this.node = node;
        this.host_announcer = announcer;

        this.discovery_lookup = CacheBuilder.newBuilder().build(new CacheLoader<URI, DiscoveryLookupClient>() {
            @Override
            public DiscoveryLookupClient load(URI key) throws Exception {
                return new HttpDiscoveryLookupClient(() -> key, node, serviceDescriptorsCodec, http);
            }
        });

        ServiceSelectorConfig config = new ServiceSelectorConfig().setPool(node.getPool());
        config.setPool(node.getPool());
        this.federation = factory.createServiceSelector(Federation.SERVICE_TYPE, config);

        // filter discovery service in service mesh
        this.discovery = factory.createServiceSelector(DISCOVERY_SERVICE_TYPE, config);
    }

    public void start() {
        Promises.schedule(//
                this::rediscovery, //
                TimeUnit.SECONDS.toMillis(5), //
                true
        ).logException();
    }

    protected void rediscovery() {
        // find native discovery
        Set<URI> discovery_services = discovery.selectAllServices().parallelStream()
                .map((service) -> {
                    return URI.create(service.getProperties().get(Federation.HTTP_URI_PROPERTY));
                })
                .distinct()
                .collect(Collectors.toSet());

        // find this application id
        Set<String> applicaiton_ids = host_announcer.getServiceAnnouncements().parallelStream()
                .filter((service) -> {
                    return service.getType().equals(SERVICE_TYPE);
                }) //
                .map((service) -> {
                    return service.getProperties().get(APPLICATION_PROPERTY);
                }) //
                .collect(Collectors.toSet());

        this.federation.selectAllServices().parallelStream() //
                // exclude self
                .filter((service) -> !service.getNodeId().equals(node))
                // collect all discovery server
                .map((discovery) -> URI.create(discovery.getProperties().get(Federation.HTTP_URI_PROPERTY))) //
                // filter native discovery
                .filter((discovery) -> !discovery_services.contains(discovery_services))
                .forEach((uri) -> {
                    // find rediscovery service
                    Promises.decorate(discovery_lookup.getUnchecked(uri).getServices(SERVICE_TYPE)) //
                            .callback((services, throwable) -> {
                                if (throwable != null) {
                                    LOGGER.error(throwable, "fail to access discovery service:" + uri);
                                    return;
                                }

                                // find service with same application id
                                services.getServiceDescriptors().parallelStream() //
                                        // ensure type
                                        .filter((service) -> service.getType().equals(SERVICE_TYPE))
                                        // exclude self
                                        .filter((service) -> !service.getNodeId().equals(node))
                                        // find service that match appid
                                        .filter((service) -> {
                                            String that_appid = service.getProperties().get(APPLICATION_PROPERTY);
                                            return applicaiton_ids.contains(that_appid);
                                        })
                                        .map((service) -> {
                                            return URI.create(service.getProperties().get(Federation.HTTP_URI_PROPERTY));
                                        })
                                        .forEach((discovery_uri) -> {
                                            federation_announcer.announce(discovery_uri, host_announcer.getServiceAnnouncements());
                                        });
                            });
                });
    }
}

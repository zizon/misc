package com.sf.misc.airlift;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.DiscoveryLookupClient;
import io.airlift.discovery.client.ForDiscoveryClient;
import io.airlift.discovery.client.HttpDiscoveryLookupClient;
import io.airlift.discovery.client.ServiceAnnouncement;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceDescriptorsRepresentation;
import io.airlift.http.client.HttpClient;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.json.JsonCodec;
import io.airlift.node.NodeInfo;

import java.net.URI;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class DependOnDiscoveryService {

    public static String HTTP_URI_PROPERTY = "http-external";

    public static String DISCOVERY_SERVICE_TYPE = "discovery";

    protected final String service_type;
    protected final Announcer announcer;
    protected final HttpServerInfo http;
    protected final ImmutableMap<String, String> properties;
    protected final LoadingCache<URI, DiscoveryLookupClient> discovery_lookup;

    public DependOnDiscoveryService(
            String serice_type,
            Announcer announcer,
            HttpServerInfo http,
            Map<String, String> properties,
            NodeInfo node,
            JsonCodec<ServiceDescriptorsRepresentation> serviceDescriptorsCodec,
            @ForDiscoveryClient HttpClient client
    ) {
        this.service_type = serice_type;
        this.announcer = announcer;
        this.http = http;
        this.properties = ImmutableMap.<String, String>builder().putAll(properties).build();

        this.discovery_lookup = CacheBuilder.newBuilder().build(new CacheLoader<URI, DiscoveryLookupClient>() {
            @Override
            public DiscoveryLookupClient load(URI key) throws Exception {
                return new HttpDiscoveryLookupClient(() -> key, node, serviceDescriptorsCodec, client);
            }
        });
    }


    public static URI http(ServiceDescriptor service) {
        return URI.create(service.getProperties().get(HTTP_URI_PROPERTY));
    }

    public Announcer announcer() {
        return announcer;
    }

    public Set<ServiceAnnouncement> privateAnnouncements() {
        return announcer.getServiceAnnouncements().parallelStream()
                .filter((service) -> service.getType().equals(service_type))
                .collect(Collectors.toSet());
    }

    public void activate() {
        if (discoveryEnabled()) {
            announceHTTPService();
        }
    }

    public DiscoveryLookupClient discoveryClient(URI uri) {
        return this.discovery_lookup.getUnchecked(uri);
    }

    protected boolean discoveryEnabled() {
        return announcer.getServiceAnnouncements().parallelStream() //
                .filter((announcement) -> {
                    return announcement.getType().equals(DISCOVERY_SERVICE_TYPE);
                }).findAny().isPresent();
    }

    protected void announceHTTPService() {
        // annouce service
        announcer.addServiceAnnouncement(//
                ServiceAnnouncement //
                        .serviceAnnouncement(service_type) //
                        .addProperties(properties)
                        .addProperty( //
                                HTTP_URI_PROPERTY, //
                                http.getHttpExternalUri().toString() //
                        ) //
                        .build() //
        );
    }
}

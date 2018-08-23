package com.sf.misc.airlift;

import com.google.common.collect.ImmutableMap;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.ServiceAnnouncement;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.http.server.HttpServerInfo;

import javax.annotation.PostConstruct;
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

    public DependOnDiscoveryService(String serice_type, Announcer announcer, HttpServerInfo http, Map<String, String> properties) {
        this.service_type = serice_type;
        this.announcer = announcer;
        this.http = http;
        this.properties = ImmutableMap.<String, String>builder().putAll(properties).build();
    }

    public static URI http(ServiceDescriptor service) {
        return URI.create(service.getProperties().get(HTTP_URI_PROPERTY));
    }

    public Announcer announcer() {
        return announcer;
    }

    public Set<ServiceAnnouncement> announcements() {
        return announcer.getServiceAnnouncements().parallelStream()
                .filter((service) -> service.getType().equals(service_type))
                .collect(Collectors.toSet());
    }

    public void activate() {
        if (discoveryEnabled()) {
            announceHTTPService();
        }
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

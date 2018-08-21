package com.sf.misc.yarn.rediscovery;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;
import com.sf.misc.airlift.federation.Federation;
import com.sf.misc.async.Promises;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.DiscoveryLookupClient;
import io.airlift.discovery.client.ForDiscoveryClient;
import io.airlift.discovery.client.HttpDiscoveryLookupClient;
import io.airlift.discovery.client.ServiceAnnouncement;
import io.airlift.discovery.client.ServiceDescriptorsRepresentation;
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

public class YarnRediscovery {

    public static final Logger LOGGER = Logger.get(YarnRediscovery.class);

    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface ForYarnRediscovery {
    }

    public static final String SERVICE_TYPE = "yarn-rediscovery";
    public static final String APPLICATION_PROPERTY = "application";

    protected static String DISCOVERY_SERVICE_TYPE = "discovery";

    protected final LoadingCache<URI, DiscoveryLookupClient> discovery_lookup;
    protected final Announcer host_announcer;
    protected final Federation federation;
    protected final NodeInfo node;
    protected final String application;
    protected final HttpServerInfo http;

    @Inject
    public YarnRediscovery(Announcer announcer,
                           NodeInfo node,
                           JsonCodec<ServiceDescriptorsRepresentation> serviceDescriptorsCodec,
                           @ForDiscoveryClient HttpClient http,
                           ServiceSelectorFactory factory,
                           Federation federation,
                           @ForYarnRediscovery String application,
                           HttpServerInfo httpServerInfo
    ) {
        this.host_announcer = announcer;
        this.federation = federation;
        this.node = node;
        this.application = application;
        this.http = httpServerInfo;

        this.discovery_lookup = CacheBuilder.newBuilder().build(new CacheLoader<URI, DiscoveryLookupClient>() {
            @Override
            public DiscoveryLookupClient load(URI key) throws Exception {
                return new HttpDiscoveryLookupClient(() -> key, node, serviceDescriptorsCodec, http);
            }
        });
    }

    @PostConstruct
    public void start() {
        // skip if not fedration provider
        if (!federation.shouldAnnouceFederation()) {
            return;
        }

        annouceYarnRediscovery();

        Promises.schedule(//
                this::rediscovery, //
                TimeUnit.SECONDS.toMillis(5), //
                true
        ).logException();
    }

    protected void annouceYarnRediscovery() {
        // annouce federation service
        host_announcer.addServiceAnnouncement(//
                ServiceAnnouncement //
                        .serviceAnnouncement(SERVICE_TYPE) //
                        .addProperty( //
                                Federation.HTTP_URI_PROPERTY, //
                                http.getHttpExternalUri().toString() //
                        ) //
                        .addProperty(
                                APPLICATION_PROPERTY,
                                application
                        )
                        .build() //
        );
    }


    protected void rediscovery() {
        // find native discovery
        Set<URI> discovery_services = this.federation.discoverySelector().selectAllServices().parallelStream()
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

        // annouce to one that not in discovery but under a same applicaiton id
        this.federation.federationSelector().selectAllServices().parallelStream() //
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
                                            federation.announcer().announce(discovery_uri, host_announcer.getServiceAnnouncements());
                                        });
                            });
                });
    }
}

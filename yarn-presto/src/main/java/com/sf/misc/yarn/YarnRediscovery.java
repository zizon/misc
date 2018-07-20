package com.sf.misc.yarn;

import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.sf.misc.airlift.UnionDiscovery;
import com.sf.misc.airlift.UnionServiceSelector;
import com.sf.misc.async.Promises;
import io.airlift.discovery.client.Announcement;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.DiscoveryAnnouncementClient;
import io.airlift.discovery.client.DiscoveryLookupClient;
import io.airlift.discovery.client.ForDiscoveryClient;
import io.airlift.discovery.client.HttpDiscoveryAnnouncementClient;
import io.airlift.discovery.client.HttpDiscoveryLookupClient;
import io.airlift.discovery.client.ServiceAnnouncement;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceDescriptorsRepresentation;
import io.airlift.discovery.client.ServiceSelectorFactory;
import io.airlift.http.client.HttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class YarnRediscovery {

    public static final Logger LOGGER = Logger.get(YarnRediscovery.class);

    public static final String SERVICE_TYPE = "yarn-rediscovery";
    public static final String APPLICATION_PROPERTY = "application";

    protected UnionServiceSelector union_discovery_selector;
    protected Promises.TransformFunction<URI, DiscoveryLookupClient> discovery_lookup;
    protected NodeInfo node;
    protected Announcer announcer;

    protected Supplier<Set<ServiceAnnouncement>> annoucement_provider;
    protected Promises.TransformFunction<URI, DiscoveryAnnouncementClient> client_provider;

    @Inject
    public YarnRediscovery(NodeInfo nodeInfo,  //
                           JsonCodec<ServiceDescriptorsRepresentation> serviceDescriptorsCodec, //
                           JsonCodec<Announcement> announcementCodec, //
                           @ForDiscoveryClient HttpClient httpClient,//
                           Announcer announcer,
                           UnionServiceSelector union_discovery_selector

    ) {
        this.announcer = announcer;
        this.node = nodeInfo;
        this.union_discovery_selector = union_discovery_selector;

        this.discovery_lookup = (discovery_uri) -> new HttpDiscoveryLookupClient(() -> discovery_uri, nodeInfo, serviceDescriptorsCodec, httpClient);

        this.annoucement_provider = () -> {
            return announcer.getServiceAnnouncements();
        };

        this.client_provider = (uri) -> {
            return new HttpDiscoveryAnnouncementClient(() -> uri, nodeInfo, announcementCodec, httpClient);
        };
    }

    public void start() {
        Promises.schedule(//
                this::rediscovery, //
                TimeUnit.SECONDS.toMillis(5) //
        ).callback((ignore, throwable) -> {
            if (throwable != null) {
                LOGGER.error(throwable, "fail to do rediscovery");
            }
        });
    }

    protected void rediscovery() {
        this.union_discovery_selector.selectAllServices().parallelStream() //
                .map((descriptor) -> {
                    return URI.create(descriptor //
                            .getProperties() //
                            .get(UnionDiscovery.HTTP_URI_PROPERTY) //
                    );
                }) //
                .distinct() //
                .map((uri) -> {
                    // find all discovery service
                    return Promises.decorate(discovery_lookup.apply(uri).getServices(SERVICE_TYPE)) //
                            .transform((descriptors) -> {
                                // ensure service type
                                return descriptors.getServiceDescriptors().parallelStream() //
                                        .filter((single) -> {
                                            if (!single.getType().equals(SERVICE_TYPE)) {
                                                return false;
                                            }

                                            return true;
                                        }).collect(Collectors.toList());
                            });
                }) //
                .reduce(Promises.reduceCollectionsOperator())
                .map((prmoise) -> {
                    // eliminate duplicated
                    return prmoise.transform((services) -> {
                        ConcurrentMap<String, ServiceDescriptor> uniq = Maps.newConcurrentMap();
                        services.parallelStream().forEach((service) -> {
                            uniq.put(service.getNodeId(), service);
                        });

                        return uniq.values();
                    });
                }) //
                .orElse(Promises.immediate(Collections.emptySet()))
                .transform((descriptors) -> {
                    // find this application id
                    Optional<String> optional = descriptors.parallelStream().filter((descriptor) -> {
                        return descriptor.getNodeId().equals(node.getNodeId());
                    }).map((descriptor) -> {
                        return descriptor.getProperties().get(APPLICATION_PROPERTY);
                    }).findAny();

                    if (!optional.isPresent()) {
                        LOGGER.warn("no application id for servier:" + SERVICE_TYPE + " may be this node:" + node + "not register yet");
                        return Collections.<URI>emptySet();
                    }

                    String applicaiton = optional.get();
                    // then find this application discoverys
                    return descriptors.parallelStream() //
                            .filter((descriptor) -> {
                                // skip this
                                if (descriptor.getNodeId().equals(node.getNodeId())) {
                                    return false;
                                }

                                return descriptor.getProperties().get(APPLICATION_PROPERTY).equals(applicaiton);
                            }) //
                            .map((descriptor) -> {
                                return URI.create(descriptor.getProperties().get(UnionDiscovery.HTTP_URI_PROPERTY));
                            }) //
                            .distinct()
                            .collect(Collectors.toSet());
                }) //
                .callback((uris, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error(throwable, "fail when re-discovery yarn application discovery for node:" + node);
                        return;
                    }

                    // annouce to this discoverys
                    uris.parallelStream().forEach((discovery_uri) -> {
                        Promises.decorate( //
                                new Announcer( //
                                        client_provider.apply(discovery_uri), //
                                        annoucement_provider.get() //
                                ).forceAnnounce() //
                        ).callback((ignore, exception) -> {
                            if (exception != null) {
                                LOGGER.error(exception, "fail to annouce to:" + discovery_uri);
                            }
                        });
                    });
                });
    }
}

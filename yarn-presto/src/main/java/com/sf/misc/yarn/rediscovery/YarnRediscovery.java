package com.sf.misc.yarn.rediscovery;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.sf.misc.airlift.DiscoverySelectors;
import com.sf.misc.airlift.UnionDiscovery;
import com.sf.misc.airlift.UnionDiscoveryConfig;
import com.sf.misc.async.Promises;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.DiscoveryLookupClient;
import io.airlift.discovery.client.ForDiscoveryClient;
import io.airlift.discovery.client.HttpDiscoveryLookupClient;
import io.airlift.discovery.client.ServiceAnnouncement;
import io.airlift.discovery.client.ServiceDescriptorsRepresentation;
import io.airlift.http.client.HttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;

import java.net.URI;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class YarnRediscovery {

    public static final Logger LOGGER = Logger.get(YarnRediscovery.class);

    public static final String SERVICE_TYPE = "yarn-rediscovery";
    public static final String APPLICATION_PROPERTY = "application";


    protected final UnionDiscovery union_discovery;
    protected final UnionDiscoveryConfig union_discovery_config;
    protected final Supplier<Set<ServiceAnnouncement>> annoucement_provider;
    protected final DiscoverySelectors discovery_selectors;
    protected final LoadingCache<URI, DiscoveryLookupClient> discovery_lookup;
    protected final NodeInfo node;
    protected final Announcer announcer;

    @Inject
    public YarnRediscovery(Announcer announcer,
                           UnionDiscovery union_discovery, //
                           UnionDiscoveryConfig union_disconvery_config,
                           DiscoverySelectors discovery_selectors,
                           NodeInfo nodeInfo,
                           JsonCodec<ServiceDescriptorsRepresentation> serviceDescriptorsCodec,
                           @ForDiscoveryClient HttpClient httpClient
    ) {
        this.union_discovery = union_discovery;
        this.union_discovery_config = union_disconvery_config;
        this.discovery_selectors = discovery_selectors;
        this.node = nodeInfo;
        this.announcer = announcer;

        this.annoucement_provider = () -> {
            return announcer.getServiceAnnouncements() //
                    .parallelStream() //
                    .filter((announcement) -> {
                        return announcement.getType().equals(SERVICE_TYPE);
                    })
                    .collect(Collectors.toSet());
        };

        this.discovery_lookup = CacheBuilder.newBuilder().build(new CacheLoader<URI, DiscoveryLookupClient>() {
            @Override
            public DiscoveryLookupClient load(URI key) throws Exception {
                return new HttpDiscoveryLookupClient(() -> key, nodeInfo, serviceDescriptorsCodec, httpClient);
            }
        });
    }

    public void start() {
        schedule(this::broadcastUnionDiscovery);
        schedule(this::rediscovery);
    }

    protected void schedule(Promises.PromiseRunnable runnable) {
        Promises.schedule(//
                runnable, //
                TimeUnit.SECONDS.toMillis(5) //
        ).callback((ignore, throwable) -> {
            if (throwable != null) {
                LOGGER.error(throwable, "fail to do rediscovery");
            }
        });
    }

    protected void broadcastUnionDiscovery() {
        String foreign_discovery = union_discovery_config.getForeignDiscovery();
        if (foreign_discovery != null) {
            union_discovery.forceAnnouce(URI.create(foreign_discovery), annoucement_provider.get());
        }
    }

    protected void rediscovery() {
        Stream.concat(discovery_selectors.local().selectAllServices().parallelStream(),//
                discovery_selectors.foreign().selectAllServices().parallelStream() //
        ).parallel()
                .map((service) -> URI.create(service.getProperties().get(UnionDiscovery.HTTP_URI_PROPERTY)))
                .distinct()
                .map((uri) -> {
                    return Promises.decorate(discovery_lookup.getUnchecked(uri).getServices(SERVICE_TYPE))
                            .transform((descriptors) -> Lists.newArrayList( //
                                    descriptors.getServiceDescriptors().parallelStream() //
                                            .filter((descriptor) -> {
                                                return descriptor.getType().equals(SERVICE_TYPE)
                                                        ;
                                            }).iterator() //
                                    )
                            );
                }) //
                .reduce(Promises.reduceCollectionsOperator())
                .orElse(Promises.immediate(Lists.newArrayList()))
                .callback((services, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error(throwable, "fail to fetch application node:" + node + " for re-discovery");
                        return;
                    }

                    Optional<String> appid = services.parallelStream()
                            .filter((service) -> service.getNodeId().equals(node.getNodeId()))
                            .map((service) -> service.getProperties().get(YarnRediscovery.APPLICATION_PROPERTY))
                            .findAny();
                    if (!appid.isPresent()) {
                        LOGGER.warn("find no application id for this applicaiton node:" + node);
                        return;
                    }

                    services.parallelStream().parallel() //
                            .filter((service) -> {
                                return !service.getNodeId().equals(node.getNodeId())
                                        && service.getProperties().get(YarnRediscovery.APPLICATION_PROPERTY).equals(appid.get());

                            })
                            .map((service) -> {
                                return URI.create(service.getProperties().get(UnionDiscovery.HTTP_URI_PROPERTY));
                            }) //
                            .distinct() //
                            .forEach((uri) -> {
                                union_discovery.forceAnnouce(uri, announcer.getServiceAnnouncements());
                            });
                });
    }


}

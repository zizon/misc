package com.sf.misc.yarn.rediscovery;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;
import com.sf.misc.airlift.AirliftConfig;
import com.sf.misc.airlift.DependOnDiscoveryService;
import com.sf.misc.airlift.federation.Federation;
import com.sf.misc.airlift.federation.ServiceSelectors;
import com.sf.misc.async.Entrys;
import com.sf.misc.async.ListenablePromise;
import com.sf.misc.async.Promises;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.DiscoveryClientConfig;
import io.airlift.discovery.client.DiscoveryLookupClient;
import io.airlift.discovery.client.ForDiscoveryClient;
import io.airlift.discovery.client.HttpDiscoveryLookupClient;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceDescriptorsRepresentation;
import io.airlift.discovery.server.DiscoveryConfig;
import io.airlift.http.client.HttpClient;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;

import javax.annotation.PostConstruct;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class YarnRediscovery extends DependOnDiscoveryService {

    public static final Logger LOGGER = Logger.get(YarnRediscovery.class);

    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface ForYarnRediscovery {
    }

    protected static final String SERVICE_TYPE = "yarn-rediscovery";
    protected static final String GROUP_PROPERTY = "group";

    protected final ServiceSelectors selectors;
    protected final Federation federation;
    protected final NodeInfo node;
    protected final String group;
    protected final URI federation_uri;
    protected final DiscoveryClientConfig discovery_client_config;

    @Inject
    public YarnRediscovery(Announcer announcer,
                           NodeInfo node,
                           JsonCodec<ServiceDescriptorsRepresentation> serviceDescriptorsCodec,
                           @ForDiscoveryClient HttpClient http,
                           Federation federation,
                           @ForYarnRediscovery String group,
                           HttpServerInfo httpServerInfo,
                           ServiceSelectors selectors,
                           AirliftConfig airlift_config,
                           DiscoveryClientConfig discovery_client_config
    ) {
        super(SERVICE_TYPE, announcer, httpServerInfo, ImmutableMap.of(GROUP_PROPERTY, group), node, serviceDescriptorsCodec, http);
        this.federation = federation;
        this.node = node;
        this.group = group;
        this.selectors = selectors;
        this.federation_uri = URI.create(airlift_config.getFederationURI());
        this.discovery_client_config = discovery_client_config;
    }

    @PostConstruct
    public void start() {
        this.activate();

        LOGGER.info("start rediscovery for discovery server...");
        Promises.schedule(//
                this::joinGroup, //
                TimeUnit.SECONDS.toMillis(5), //
                true
        ).logException();

        if (!discoveryEnabled()) {
            LOGGER.info("discovery not enable, try find one");
            Promises.schedule(//
                    this::findDiscovery, //
                    TimeUnit.SECONDS.toMillis(5), //
                    true
            ).logException();
        }
    }


    protected void joinGroup() {
        LOGGER.debug("scheudle one rediscovery...");

        // find native discovery
        Set<URI> native_discovery_services = selectors.selectServiceForType(DISCOVERY_SERVICE_TYPE).parallelStream()
                .map(DependOnDiscoveryService::http)
                .collect(Collectors.toSet());
        LOGGER.debug("find native discovery service:" + native_discovery_services);

        // find all discovery server
        selectors.selectServiceForType(Federation.SERVICE_TYPE).parallelStream()
                .map(Federation::http)
                .distinct()
                // exclude native discovery
                .filter((discovery) -> !native_discovery_services.contains(discovery))
                .forEach((discovery_uri) -> {
                    // then ask for foriengn service list
                    LOGGER.debug("ask foriengn discovery for re-discovery:" + discovery_uri);

                    // re-discovery service
                    this.services(discovery_uri, SERVICE_TYPE)
                            .callback((services) -> {
                                // filter services
                                services.parallelStream() //
                                        // exclude self
                                        .filter((service) -> !service.getNodeId().equals(node.getNodeId()))
                                        // ensure type
                                        .filter((service) -> service.getType().equals(SERVICE_TYPE))
                                        // find same node group
                                        .filter((service) -> group.equals(service.getProperties().get(GROUP_PROPERTY)))
                                        .findAny()
                                        .ifPresent((ignore) -> {
                                            LOGGER.debug("join discvoery group:" + discovery_uri);
                                            federation.annouce(discovery_uri, announcer().getServiceAnnouncements());
                                        });
                            })
                            .logException((ignore) -> "fail to fetch service list from:" + discovery_uri);
                });
    }

    protected void findDiscovery() {
        // find federations
        services(federation_uri, Federation.SERVICE_TYPE)
                .transformAsync((federations) -> {
                    return federations.parallelStream()
                            .map(Federation::http)
                            .distinct()
                            .map((uri) -> services(uri, SERVICE_TYPE)
                                    .transform((services) -> {
                                        return services.parallelStream()
                                                // exclude self
                                                .filter((service) -> !service.getNodeId().equals(node.getNodeId()))
                                                // ensure type
                                                .filter((service) -> service.getType().equals(SERVICE_TYPE))
                                                // find same node group
                                                .filter((service) -> group.equals(service.getProperties().get(GROUP_PROPERTY)))
                                                // to uri
                                                .map(Federation::http)
                                                // collect
                                                .collect(Collectors.toSet());
                                    })
                            )
                            .reduce(Promises.reduceCollectionsOperator())
                            .orElseGet(() -> Promises.immediate(Collections.emptySet()));
                }) //
                .callback((uris) -> {
                    uris.parallelStream()
                            .sorted()
                            .findFirst()
                            .ifPresent((uri) -> {
                                discovery_client_config.setDiscoveryServiceURI(uri);
                                LOGGER.debug("using discovery uri:" + uri + " for rediscovery");
                            });
                }).logException((ignore) -> "fail to define discovery uri for rediscovery service");
    }
}

package com.sf.misc.airlift.federation;

import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.sf.misc.airlift.AirliftConfig;
import com.sf.misc.airlift.DependOnDiscoveryService;
import com.sf.misc.async.ListenablePromise;
import com.sf.misc.async.Promises;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.ForDiscoveryClient;
import io.airlift.discovery.client.ServiceAnnouncement;
import io.airlift.discovery.client.ServiceDescriptorsRepresentation;
import io.airlift.http.client.HttpClient;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;

import javax.annotation.PostConstruct;
import java.net.URI;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Federation extends DependOnDiscoveryService {

    public static Logger LOGGER = Logger.get(Federation.class);

    public static final String SERVICE_TYPE = "federation";

    protected final ServiceSelectors selectors;
    protected final FederationAnnouncer fedration_announcer;
    protected final AirliftConfig airlift_config;

    @Inject
    public Federation(Announcer announcer,
                      FederationAnnouncer fedration_announcer,
                      AirliftConfig airlift_config,
                      HttpServerInfo httpServerInfo,
                      ServiceSelectors selectors,
                      NodeInfo node,
                      JsonCodec<ServiceDescriptorsRepresentation> serviceDescriptorsCodec,
                      @ForDiscoveryClient HttpClient http

    ) {
        super(SERVICE_TYPE, announcer, httpServerInfo, Collections.emptyMap(), node, serviceDescriptorsCodec, http);
        this.fedration_announcer = fedration_announcer;
        this.airlift_config = airlift_config;
        this.selectors = selectors;
    }

    public void annouce(URI federation_uri, Set<ServiceAnnouncement> announcements) {
        this.fedration_announcer.announce(federation_uri, announcements);
    }

    @PostConstruct
    public void start() {
        this.activate();

        if (!discoveryEnabled()) {
            return;
        }

        // propogate federation service in all discovery server
        Promises.schedule(() -> {
                    // collect discovery server
                    Set<URI> local_discovery = selectors.selectServiceForType(DISCOVERY_SERVICE_TYPE).parallelStream()
                            .map(DependOnDiscoveryService::http)
                            .collect(Collectors.toSet());
                    LOGGER.debug("local discovery:" + local_discovery);

                    ListenablePromise<Set<URI>> federations = selectors.selectServiceForType(SERVICE_TYPE).parallelStream()
                            // collect federation
                            .map(DependOnDiscoveryService::http)
                            .distinct()
                            // then aggressive collect federation from current federation set,
                            // in case one can find each other
                            .map((federation_discovery_uri) -> {
                                return this.services(federation_discovery_uri, SERVICE_TYPE) //
                                        .transform((services) -> {
                                            return services.parallelStream()
                                                    .map((service) -> http(service))
                                                    .collect(Collectors.toSet());
                                        });
                            }) //
                            .reduce(Promises.reduceCollectionsOperator())
                            .orElse(Promises.immediate(Collections.emptySet()));

                    // broadcast remote?
                    String raw_uri = airlift_config.getFederationURI();
                    if (raw_uri != null) {
                        // add foreign discovery to proper set
                        URI static_federation = URI.create(raw_uri);
                        if (!local_discovery.contains(static_federation)) {
                            federations = federations.transform((uris) -> {
                                LOGGER.debug("add statit_federation:" + static_federation);
                                uris.add(static_federation);
                                return uris;
                            });
                        }
                    }

                    // federation to annouce
                    Set<ServiceAnnouncement> announcements = privateAnnouncements();

                    // anounce federation service to all
                    // exclude discovery nodes in this discovery group.
                    // since discovery replication will do that thing.
                    federations.callback((uris) -> {
                        LOGGER.debug("all discovery:" + uris);
                        Sets.difference(uris, local_discovery).parallelStream() //
                                .distinct() //
                                .forEach((discovery_uri) -> {
                                    LOGGER.debug("annouce to federation:" + discovery_uri);
                                    fedration_announcer.announce(discovery_uri, announcements);
                                });
                    });
                }, //
                TimeUnit.SECONDS.toMillis(5),
                true
        ).logException();
    }
}

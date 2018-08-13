package com.sf.misc.airlift.federation;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.sf.misc.airlift.AirliftConfig;
import com.sf.misc.async.ListenablePromise;
import com.sf.misc.async.Promises;
import io.airlift.discovery.client.Announcement;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.ForDiscoveryClient;
import io.airlift.discovery.client.HttpDiscoveryAnnouncementClient;
import io.airlift.discovery.client.ServiceAnnouncement;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.discovery.client.ServiceSelectorConfig;
import io.airlift.discovery.client.ServiceSelectorFactory;
import io.airlift.http.client.HttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;

import javax.annotation.PostConstruct;
import java.net.URI;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Federation {

    public static Logger LOGGER = Logger.get(Federation.class);

    public static String SERVICE_TYPE = "federation";
    public static String HTTP_URI_PROPERTY = "http-external";

    protected static String DISCOVERY_SERVICE_TYPE = "discovery";

    protected final ConcurrentMap<URI, Announcer> announcers;
    protected final NodeInfo node;

    protected final Announcer host_announcer;
    protected final FederationAnnouncer fedration_announcer;
    protected final AirliftConfig airlift_config;

    protected final ServiceSelector federation;
    protected final ServiceSelector discovery;

    @Inject
    public Federation(Announcer announcer,
                      FederationAnnouncer fedration_announcer,
                      NodeInfo nodeInfo,
                      AirliftConfig airlift_config,
                      ServiceSelectorFactory factory
    ) {
        this.fedration_announcer = fedration_announcer;
        this.announcers = Maps.newConcurrentMap();
        this.host_announcer = announcer;
        this.node = nodeInfo;
        this.airlift_config = airlift_config;

        // prepare selector config
        ServiceSelectorConfig config = new ServiceSelectorConfig();
        config.setPool(nodeInfo.getPool());

        // filter federation service in service mesh
        this.federation = factory.createServiceSelector(SERVICE_TYPE, config);

        // filter discovery service in service mesh
        this.discovery = factory.createServiceSelector(DISCOVERY_SERVICE_TYPE, config);
    }

    @PostConstruct
    public void start() {
        Promises.schedule(() -> {
                    // collect discovery
                    Set<URI> local_discovery = discovery.selectAllServices().parallelStream()
                            .map((descriptor) -> URI.create(descriptor.getProperties().get(HTTP_URI_PROPERTY)))
                            .collect(Collectors.toSet());

                    // collect federation
                    Set<URI> foreign_discovery = federation.selectAllServices().parallelStream()
                            .map((descriptor) -> URI.create(descriptor.getProperties().get(HTTP_URI_PROPERTY)))
                            .collect(Collectors.toSet());

                    // find federation announcement
                    Set<ServiceAnnouncement> announcements = host_announcer.getServiceAnnouncements().parallelStream() //
                            .filter((announcement) -> {
                                return announcement.getType().equals(SERVICE_TYPE);
                            }).collect(Collectors.toSet());

                    // broadcast remote?
                    String raw_uri = airlift_config.getForeignDiscovery();
                    if (raw_uri != null) {
                        URI static_federation = URI.create(raw_uri);
                        if (!local_discovery.contains(static_federation)) {
                            foreign_discovery.add(static_federation);
                        }
                    }

                    // anounce federation service to all
                    // exclude discovery nodes in this discovery group.
                    // since discovery replication will do that thing.
                    Sets.difference(foreign_discovery, local_discovery).parallelStream() //
                            .distinct() //
                            .forEach((discovery_uri) -> {
                                fedration_announcer.announce(discovery_uri, announcements);
                            });
                }, //
                TimeUnit.SECONDS.toMillis(5),
                true
        ).logException();
    }
}

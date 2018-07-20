package com.sf.misc.airlift;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.sf.misc.async.ListenablePromise;
import com.sf.misc.async.Promises;
import io.airlift.discovery.client.Announcement;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.DiscoveryAnnouncementClient;
import io.airlift.discovery.client.ForDiscoveryClient;
import io.airlift.discovery.client.HttpDiscoveryAnnouncementClient;
import io.airlift.discovery.client.ServiceAnnouncement;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.discovery.client.ServiceSelectorConfig;
import io.airlift.discovery.client.ServiceSelectorFactory;
import io.airlift.http.client.HttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class UnionDiscovery {

    public static Logger LOGGER = Logger.get(UnionDiscovery.class);

    public static String SERVICE_TYPE = "union-discovery";
    public static String DISCOVERY_SERVICE_TYPE = "discovery";
    public static String HTTP_URI_PROPERTY = "http-external";

    protected Supplier<Set<ServiceAnnouncement>> annoucement_provider;
    protected UnionServiceSelector union_discovery_selector;
    protected Promises.TransformFunction<URI, DiscoveryAnnouncementClient> client_provider;

    @Inject
    public UnionDiscovery(Announcer announcer,
                          NodeInfo nodeInfo,
                          JsonCodec<Announcement> announcementCodec,
                          @ForDiscoveryClient HttpClient httpClient,
                          UnionServiceSelector union_discovery_selector
    ) {
        this.union_discovery_selector = union_discovery_selector;

        this.annoucement_provider = () -> {
            return announcer.getServiceAnnouncements() //
                    .parallelStream() //
                    .filter((announcement) -> {
                        return announcement.getType().equals(SERVICE_TYPE);
                    })
                    .collect(Collectors.toSet());
        };

        this.client_provider = (uri) -> {
            return new HttpDiscoveryAnnouncementClient(() -> uri, nodeInfo, announcementCodec, httpClient);
        };
    }

    public void start() {
        Promises.schedule(() -> {
                    union_discovery_selector.selectAllServices().parallelStream() //
                            .map((descriptor) -> {
                                return URI.create(descriptor.getProperties().get(HTTP_URI_PROPERTY));
                            }) //
                            .distinct() //
                            .forEach((discovery_uri) -> {
                                Promises.decorate(
                                        new Announcer( //
                                                client_provider.apply(discovery_uri), //
                                                annoucement_provider.get() //
                                        ).forceAnnounce()
                                ).callback((ignore, throwable) -> {
                                    if (throwable != null) {
                                        LOGGER.error(throwable, "fail to annouce union discovery to discovery:" + discovery_uri);
                                        return;
                                    }
                                });
                            });
                }, //
                TimeUnit.SECONDS.toMillis(5)
        ).callback((ignore, throwable) -> {
            if (throwable != null) {
                LOGGER.error(throwable, "fail when brocast discovery");
            }
        });
    }
}

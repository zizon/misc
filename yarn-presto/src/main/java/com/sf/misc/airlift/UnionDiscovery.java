package com.sf.misc.airlift;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.sf.misc.async.ListenablePromise;
import com.sf.misc.async.Promises;
import io.airlift.discovery.client.Announcement;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.DiscoveryAnnouncementClient;
import io.airlift.discovery.client.ForDiscoveryClient;
import io.airlift.discovery.client.HttpDiscoveryAnnouncementClient;
import io.airlift.discovery.client.ServiceAnnouncement;
import io.airlift.http.client.HttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;

import java.net.URI;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class UnionDiscovery {

    public static Logger LOGGER = Logger.get(UnionDiscovery.class);

    public static String SERVICE_TYPE = "union-discovery";
    public static String DISCOVERY_SERVICE_TYPE = "discovery";
    public static String HTTP_URI_PROPERTY = "http-external";

    protected final Supplier<Set<ServiceAnnouncement>> annoucement_provider;
    protected final DiscoverySelectors union_selector;
    protected final LoadingCache<URI, Announcer> announcers;

    @Inject
    public UnionDiscovery(Announcer announcer,
                          NodeInfo nodeInfo,
                          JsonCodec<Announcement> announcementCodec,
                          @ForDiscoveryClient HttpClient httpClient,
                          DiscoverySelectors union_selector
    ) {
        this.union_selector = union_selector;

        this.announcers = CacheBuilder.newBuilder().build(new CacheLoader<URI, Announcer>() {
            @Override
            public Announcer load(URI key) throws Exception {
                return new Announcer(
                        new HttpDiscoveryAnnouncementClient(() -> key, nodeInfo, announcementCodec, httpClient), //
                        Collections.emptySet() //
                );
            }
        });

        this.annoucement_provider = () -> {
            return announcer.getServiceAnnouncements() //
                    .parallelStream() //
                    .filter((announcement) -> {
                        return announcement.getType().equals(SERVICE_TYPE);
                    })
                    .collect(Collectors.toSet());
        };
    }

    public Announcer announcer(URI uri) {
        return this.announcers.getUnchecked(uri);
    }

    public ListenablePromise<?> forceAnnouce(URI uri, Set<ServiceAnnouncement> announcements) {
        Announcer announcer = announcer(uri);
        announcements.parallelStream()
                .forEach(announcer::addServiceAnnouncement);

        return Promises.decorate(
                announcer.forceAnnounce()
        ).callback((ignore, throwable) -> {
            if (throwable != null) {
                LOGGER.error(throwable, "fail to annouce union discovery to discovery:" + uri);
                return;
            }
        });
    }

    public void start() {
        Promises.schedule(() -> {
                    Set<URI> local_discovery = union_selector.local().selectAllServices().parallelStream()
                            .map((descriptor) -> URI.create(descriptor.getProperties().get(HTTP_URI_PROPERTY)))
                            .collect(Collectors.toSet());

                    Set<URI> foreign_discovery = union_selector.foreign().selectAllServices().parallelStream()
                            .map((descriptor) -> URI.create(descriptor.getProperties().get(HTTP_URI_PROPERTY)))
                            .collect(Collectors.toSet());

                    Set<ServiceAnnouncement> announcements = annoucement_provider.get();
                    Sets.difference(foreign_discovery, local_discovery).parallelStream() //
                            .distinct() //
                            .forEach((discovery_uri) -> {
                                forceAnnouce(discovery_uri, announcements);
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

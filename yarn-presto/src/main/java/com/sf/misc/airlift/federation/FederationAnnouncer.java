package com.sf.misc.airlift.federation;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.sf.misc.async.Promises;
import io.airlift.discovery.client.Announcement;
import io.airlift.discovery.client.DiscoveryAnnouncementClient;
import io.airlift.discovery.client.ForDiscoveryClient;
import io.airlift.discovery.client.HttpDiscoveryAnnouncementClient;
import io.airlift.discovery.client.ServiceAnnouncement;
import io.airlift.http.client.HttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;

import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class FederationAnnouncer {

    public static final Logger LOGGER = Logger.get(FederationAnnouncer.class);

    protected final LoadingCache<URI, DiscoveryAnnouncementClient> client_cache;

    @Inject
    public FederationAnnouncer(NodeInfo node,
                               JsonCodec<Announcement> announcement_codec,
                               @ForDiscoveryClient HttpClient http) {
        this.client_cache = CacheBuilder.newBuilder().build(new CacheLoader<URI, DiscoveryAnnouncementClient>() {
            @Override
            public DiscoveryAnnouncementClient load(URI discovery_uri) throws Exception {
                return new HttpDiscoveryAnnouncementClient(() -> discovery_uri, node, announcement_codec, http);
            }
        });
    }

    public void announce(URI discovery_uri, Set<ServiceAnnouncement> announcements) {
        Promises.submit(() -> client_cache.get(discovery_uri).announce(announcements))
                .logException(() -> "fail to annouce to:" + discovery_uri + " announcement:" + announcements);
    }
}

package com.sf.misc.yarn;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.sf.misc.async.ListenablePromise;
import com.sf.misc.async.Promises;
import io.airlift.discovery.client.Announcement;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.ForDiscoveryClient;
import io.airlift.discovery.client.HttpDiscoveryAnnouncementClient;
import io.airlift.discovery.client.ServiceAnnouncement;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceInventory;
import io.airlift.http.client.HttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;

import java.net.URI;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class AirliftYarnAnnouncer {

    public static final Logger LOGGER = Logger.get(AirliftYarnAnnouncer.class);

    public static String SERVICE_TYPE = "re-discovery";
    public static String DISCOVERY_SERVICE_TYPE = "discovery";

    protected final Announcer my_announcer;
    protected final ServiceInventory my_inventory;
    protected final NodeInfo my_node;
    protected final HttpClient client;

    protected LoadingCache<URI, Promises.TransformFunction<Set<ServiceAnnouncement>, Announcer>> announcers;

    @Inject
    public AirliftYarnAnnouncer(NodeInfo nodeInfo,
                                JsonCodec<Announcement> announcementCodec,
                                @ForDiscoveryClient HttpClient httpClient,
                                Announcer announcer,
                                ServiceInventory inventory) {
        this.client = httpClient;
        this.my_node = nodeInfo;
        this.my_announcer = announcer;
        this.my_inventory = inventory;

        this.announcers = CacheBuilder.newBuilder().build(new CacheLoader<URI, Promises.TransformFunction<Set<ServiceAnnouncement>, Announcer>>() {
            @Override
            public Promises.TransformFunction<Set<ServiceAnnouncement>, Announcer> load(URI key) throws Exception {
                return (services) -> {
                    return new Announcer(
                            new HttpDiscoveryAnnouncementClient(
                                    () -> key, //
                                    nodeInfo, //
                                    announcementCodec, //
                                    httpClient //
                            ), //
                            services //
                    );
                };
            }
        });
    }

    public void start(ServiceInventory foreign_inventory) {
        Promises.schedule(() -> {
                    LOGGER.info("announcer once...");
                    ImmutableList.<ListenablePromise<?>>builder()
                            .add(startForeignAnnouncer(foreign_inventory))
                            .add(startFederationAnnouncer(foreign_inventory))
                            .build()
                            .parallelStream()
                            .forEach((promise) -> {
                                promise.callback((ignore, throwable) -> {
                                    if (throwable != null) {
                                        LOGGER.error(throwable, "fail to announcer");
                                        return;
                                    }

                                    LOGGER.info("announce ok:" + ignore);
                                });
                            });
                },  //
                TimeUnit.SECONDS.toMillis(5)) //
                .callback((ignore, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error(throwable, "start airlift yarn announcer fail");
                        return;
                    }
                });
    }

    protected ListenablePromise<?> startForeignAnnouncer(ServiceInventory foreign_inventory) {
        return Promises.submit(() -> {
            URI foreign_discovery = StreamSupport.stream(foreign_inventory.getServiceDescriptors(DISCOVERY_SERVICE_TYPE).spliterator(), true) //
                    .map((descriptor) -> {
                        return URI.create(descriptor.getProperties().get("http-external"));
                    })
                    .findFirst().orElse(null);

            // no discovery
            if (foreign_discovery == null) {
                LOGGER.warn("no foreign discovery uri found,yet");
                return Promises.immediate(null);
            }

            // annouce re-discovery to foreign initiator
            return Promises.decorate(announcers.getUnchecked(foreign_discovery)
                    .apply(my_announcer //
                            .getServiceAnnouncements()
                            .parallelStream()
                            .filter((announcement) -> announcement.getType() == SERVICE_TYPE)
                            .collect(Collectors.toSet())
                    ).forceAnnounce());
        }).transformAsync((throught) -> throught);
    }

    protected ListenablePromise<?> startFederationAnnouncer(ServiceInventory foreign_inventory) {
        return Promises.submit(() -> {
            Optional<ServiceDescriptor> my_service_descriptor = StreamSupport.stream(foreign_inventory.getServiceDescriptors(SERVICE_TYPE).spliterator(), true)
                    .filter((service) -> service.getNodeId().equals(my_node.getNodeId()))
                    .findAny();

            if (!my_service_descriptor.isPresent()) {
                // not register in foreign yet
                LOGGER.warn("no re-discovery uri found,yet");
                return Promises.immediate(null);
            }

            ServiceDescriptor descriptor = my_service_descriptor.get();
            // find federate discovery
            String key = "application";
            String appid = descriptor.getProperties().get(key);
            UUID self_id = descriptor.getId();

            return StreamSupport.stream(foreign_inventory.getServiceDescriptors(SERVICE_TYPE).spliterator(), true)
                    .filter((service) -> service.getProperties().get(key).equals(appid) && !service.getId().equals(self_id))
                    .map((service) -> {
                        URI uri = URI.create(service.getProperties().get("http-external"));
                        return Promises.decorate( //
                                announcers.getUnchecked(uri) //
                                        .apply(my_announcer.getServiceAnnouncements()) //
                                        .forceAnnounce() //
                        );
                    }) //
                    .reduce((left, right) -> {
                        return left.transformAsync((ignore) -> right);
                    }) //
                    .orElse(Promises.immediate(null));
        }).transformAsync((through) -> through);
    }

    protected ListenablePromise<?> startTrasncriptDiscovery(ServiceInventory foreign_inventory) {
        my_announcer.getServiceAnnouncements().parallelStream().filter((announcement) -> {
            return announcement.getType() == DISCOVERY_SERVICE_TYPE;
        }).map((announcement) -> {
            return ServiceAnnouncement.serviceAnnouncement(SERVICE_TYPE).addProperties(announcement.getProperties()).build();
        });

        StreamSupport.stream(foreign_inventory.getServiceDescriptors(DISCOVERY_SERVICE_TYPE).spliterator(), true)
                .map((service) -> URI.create(service.getProperties().get("http-external")))
                .forEach((uri) -> {

                });

        return null;
    }
}

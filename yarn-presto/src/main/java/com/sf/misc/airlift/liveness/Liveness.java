package com.sf.misc.airlift.liveness;

import com.google.inject.Inject;
import com.sf.misc.async.Promises;
import io.airlift.discovery.server.DynamicStore;
import io.airlift.log.Logger;

import javax.annotation.PostConstruct;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.channels.SocketChannel;
import java.util.concurrent.TimeUnit;

public class Liveness {

    public static final Logger LOGGER = Logger.get(Liveness.class);

    protected static final String HTTP_SERVICE_PROPERTY = "http-external";

    protected final DynamicStore store;

    @Inject
    public Liveness(DynamicStore store) {
        this.store = store;
    }

    @PostConstruct
    public void start() {
        LOGGER.info("start liveness ...");
        Promises.schedule(
                this::check,
                TimeUnit.SECONDS.toMillis(5),
                true
        ).logException();
    }

    protected void check() {
        LOGGER.debug("schedule a liveness...");

        // collect end points
        store.getAll().parallelStream()
                // filter service
                .filter((service) -> service.getProperties().get(HTTP_SERVICE_PROPERTY) != null)
                // to uri
                .forEach((service) -> {
                    Promises.submit(() -> {
                        URI uri = URI.create(service.getProperties().get(HTTP_SERVICE_PROPERTY));
                        try (SocketChannel channel = SocketChannel.open(new InetSocketAddress(uri.getHost(), uri.getPort()))) {
                            channel.configureBlocking(true);
                            channel.finishConnect();
                        }
                        // connection ok
                    }).logException((ignore) -> {
                        // connection fail
                        store.delete(service.getNodeId());

                        return "service " + service + "may not health, kick it off";
                    });
                });
    }
}

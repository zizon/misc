package com.sf.misc.antman.v1;

import com.sf.misc.antman.io.ByteStream;
import com.sf.misc.antman.io.FileStore;
import com.sf.misc.antman.Promise;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class StreamContextLookup {

    public static final Log LOGGER = LogFactory.getLog(StreamContextLookup.class);

    protected static final ConcurrentMap<UUID, ByteStream.StreamContext> CONTEXTS = new ConcurrentHashMap<>();

    protected static final Promise<Void> READY = Promise.promise();

    public static Promise<ByteStream.StreamContext> ask(UUID stream_id) {
        return READY.transform((ready) -> {
            ByteStream.StreamContext found = CONTEXTS.get(stream_id);
            if (found == null) {
                // try create one
                found = new ByteStream.StreamContext(stream_id);
                if (CONTEXTS.putIfAbsent(stream_id, found) != null) {
                    found = CONTEXTS.get(stream_id);
                }
            }

            return Optional.of(found).get();
        });
    }

    public static Promise<?> destroy(UUID stream_id) {
        return READY.transformAsync((ignore) -> ask(stream_id)).transformAsync((context) -> {
            return context.drop().transform((ignore) -> {
                CONTEXTS.remove(context.uuid());
                return null;
            });
        });
    }

    public static Promise<Void> startup(FileStore.MMU mmu) {
        LOGGER.info("starting stream context lookup...");
        return ByteStream.loadFromMMU(mmu) //
                .transform((contexts) -> {
                    return contexts.parallelStream().map((context) -> {
                        if (CONTEXTS.putIfAbsent(context.uuid(), context) != null) {
                            return context.drop().transform((ignore) -> null);
                        }

                        return Promise.success(null);
                    }).collect(Promise.collector())
                            .sidekick(()->LOGGER.info("recovered stream context"));
                })
                .transform((ignore) -> {
                    READY.complete(null);
                    return READY;
                })
                .catching((throwable) -> {
                    READY.completeExceptionally(throwable);
                })
                .fallback(() -> READY)
                .transformAsync((throuth) -> throuth);
    }
}

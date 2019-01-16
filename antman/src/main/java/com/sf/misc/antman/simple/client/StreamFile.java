package com.sf.misc.antman.simple.client;

import com.sf.misc.antman.Promise;
import com.sf.misc.antman.simple.Stringify;
import com.sf.misc.antman.simple.packets.StreamChunkAckPacket;
import com.sf.misc.antman.simple.packets.StreamChunkPacket;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.IOException;
import java.io.SyncFailedException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public interface StreamFile extends Stringify {

    public static final Log LOGGER = LogFactory.getLog(StreamFile.class);

    default public Promise<?> upload(long timeout, long max_retry) {
        long size = size();
        long batch = batch();
        UUID stream_id = streamID();

        long parts = size / batch +
                (size % batch > 0 ? 1 : 0);

        // uplaod chunks
        List<Promise<Optional<FileChunk>>> uplaoded = LongStream.range(0, parts).parallel()
                .mapToObj((part_id) -> {
                    return new FileChunk() {
                        @Override
                        public Promise<?> send(StreamChunkPacket packet) {
                            return StreamFile.this.send(packet);
                        }

                        @Override
                        public UUID streamID() {
                            return stream_id;
                        }

                        @Override
                        public FileChunkAcks acks() {
                            return StreamFile.this.acks();
                        }

                        @Override
                        public long offset() {
                            return part_id * batch;
                        }

                        @Override
                        public ByteBuffer content() {
                            return slice(offset(), Math.min(offset() + batch, size));
                        }
                    };
                })
                .map((chunk) -> {
                    Promise<Boolean> uploaded = chunk.upload(timeout);
                    return uploaded.<Optional<FileChunk>>transform((ok) -> {
                        if (ok) {
                            return Optional.of(chunk);
                        }

                        LOGGER.warn("upload chunk fail:" + chunk);
                        return Optional.empty();
                    });
                })
                .collect(Collectors.toList());

        // when done
        Promise<?> uplaod_done = uplaoded.parallelStream().collect(Promise.collector());

        // with fallback
        Stream<Promise<Optional<FileChunk>>> uploaded_chunks = uplaoded.parallelStream()
                .map((promise) -> promise.fallback(Optional::empty));

        // do statistics with fallback
        uplaod_done.addListener(() -> {
            long success = uploaded_chunks
                    .map((promise) -> promise.join())
                    .filter(Optional::isPresent)
                    .count();
            LOGGER.warn("upload file:" + this + " fail partially, success:(" + success + "/" + parts + ")");
        });

        // retry
        return uplaod_done;
    }


    @Override
    default public String string() {
        return "[" + this.getClass().getSimpleName() + "]:" + " stream:" + streamID() + " size:" + size() + " batch:" + batch();
    }

    public long size();

    public long batch();

    public UUID streamID();

    public FileChunkAcks acks();

    public ByteBuffer slice(long from, long to);

    public Promise<?> send(StreamChunkPacket packet);
}

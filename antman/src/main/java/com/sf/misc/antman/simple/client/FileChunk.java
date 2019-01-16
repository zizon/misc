package com.sf.misc.antman.simple.client;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.sf.misc.antman.Promise;
import com.sf.misc.antman.simple.CRC;
import com.sf.misc.antman.simple.Stringify;
import com.sf.misc.antman.simple.packets.RequestCRCPacket;
import com.sf.misc.antman.simple.packets.StreamChunkPacket;
import io.netty.channel.ChannelFuture;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public interface FileChunk extends Stringify {

    public static final Log LOGGER = LogFactory.getLog(FileChunk.class);


    static final Cache<String, Long> CRC_CACHE = CacheBuilder.newBuilder()
            .expireAfterAccess(Duration.ofMinutes(1))
            .build();

    default public Promise<Boolean> upload(long timeout) {
        UUID stream_id = streamID();
        long offset = offset();
        ByteBuffer content = content();
        return crcMatch().timeout(
                () -> {
                    LOGGER.warn("ask crc timeout, chunk:" + this);
                    send(new StreamChunkPacket(streamID(), offset, content.duplicate()));
                    return false;
                },
                timeout
        );
    }

    default Promise<Boolean> crcMatch() {
        UUID stream_id = streamID();
        long offset = offset();
        ByteBuffer content = content();

        // crc
        Promise<Long> crc = Promise.light(() -> {
            return CRC_CACHE.get(stream_id.toString() + "|" + offset, () -> CRC.crc(content.duplicate()));
        });

        // acked
        Promise<Long> acked = ackedCRC();

        return Promise.all(crc, acked).transform((ignore) -> crc.join() == acked.join());
    }

    default Promise<Long> ackedCRC() {
        long offset = offset();
        return acks().rangeAckedCRC(streamID(), offset, offset + content().remaining());
    }

    @Override
    default String string() {
        return "[" + this.getClass().getSimpleName() + "]: stream:" + streamID() + " offset:" + offset() + " length:" + content().remaining();
    }

    public Promise<?> send(StreamChunkPacket packet);

    public UUID streamID();

    public FileChunkAcks acks();

    public long offset();

    public ByteBuffer content();

}

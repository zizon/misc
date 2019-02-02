package com.sf.misc.antman.simple.packets;

import com.sf.misc.antman.Promise;
import com.sf.misc.antman.simple.server.ChunkServent;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.UUID;
import java.util.zip.CRC32;

public class CommitStreamPacket implements Packet {

    public static final Log LOGGER = LogFactory.getLog(CommitStreamPacket.class);

    protected UUID stream_id;
    protected long length;
    protected long crc;
    protected boolean match;

    public CommitStreamPacket(UUID stream_id, long length, long crc) {
        this.stream_id = stream_id;
        this.length = length;
        this.crc = crc;
    }

    protected CommitStreamPacket() {
    }

    @Override
    public void decodeComplete(ChannelHandlerContext ctx) {
        // fetch page
        Promise<ByteBuffer> fetching_page = ChunkServent.mmap(stream_id, 0, length);

        // calculate crc
        Promise<Long> my_crc = fetching_page.transform((page) -> {
            CRC32 crc = new CRC32();
            crc.update(page);

            return crc.getValue();
        });

        // unmap when crc calculated
        Promise.all(fetching_page, my_crc)
                .addListener(
                        () -> ChunkServent.unmap(fetching_page.join())
                                .logException()
                );

        // send response
        my_crc.transformAsync((calculated_crc) -> {
            boolean match = calculated_crc == crc;
            LOGGER.info("my crc:" + calculated_crc + " client crc:" + crc + " length:" + length + " file:" + ChunkServent.file(stream_id).length());
            return Promise.wrap(
                    ctx.writeAndFlush(
                            new CommitStreamAckPacket(
                                    stream_id,
                                    calculated_crc,
                                    match
                            ))
            );
        }).catching(ctx::fireExceptionCaught);
    }

    @Override
    public byte type() {
        return 0x02;
    }

    @Override
    public String toString() {
        return "Stream:" + stream_id + " crc:" + crc + " length:" + length;
    }
}

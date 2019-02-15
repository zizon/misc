package com.sf.misc.antman.simple.packets;

import com.sf.misc.antman.Promise;
import com.sf.misc.antman.simple.ChunkServent;
import io.netty.channel.ChannelHandlerContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.zip.CRC32;

public class CommitStreamPacket implements Packet {

    public static final Log LOGGER = LogFactory.getLog(CommitStreamPacket.class);

    protected UUID stream_id;
    protected long length;
    protected long crc;
    protected boolean match;
    protected UUID client_id;
    protected String file_name;

    public CommitStreamPacket(UUID stream_id, long length, long crc, UUID client_id, String file_name) {
        this.stream_id = stream_id;
        this.length = length;
        this.crc = crc;
        this.client_id = client_id;
        this.file_name = file_name;
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
            LOGGER.info(page);
            return crc.getValue();
        });

        // unmap when crc calculated
        Promise.all(fetching_page, my_crc)
                .addListener(
                        () -> ChunkServent.unmap(fetching_page.join())
                                .logException()
                );

        // match?
        Promise<Boolean> commited = my_crc.transformAsync((local_crc) -> {
            if (local_crc != crc) {
                return Promise.success(false);
            }

            return ChunkServent.commit(stream_id, client_id).transform((ignore) -> true);
        });

        // send response
        Promise.all(my_crc, commited).transformAsync((ignore) -> {
            long calculated_crc = my_crc.join();
            boolean commit = commited.join();
            boolean match = calculated_crc == crc && commit;
            LOGGER.info("stream:" + stream_id + " my crc:" + calculated_crc + " client crc:" + crc + " length:" + length + " file:" + ChunkServent.file(stream_id).length() + " commit:" + commit);

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

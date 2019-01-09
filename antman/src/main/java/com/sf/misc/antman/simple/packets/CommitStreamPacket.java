package com.sf.misc.antman.simple.packets;

import com.sf.misc.antman.simple.Packet;
import com.sf.misc.antman.simple.server.ChunkServent;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
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
        try {
            CRC32 my_crc = new CRC32();
            my_crc.update(ChunkServent.mmap(stream_id, 0, length));

            this.match = my_crc.getValue() == crc;

            CommitStreamAckPacket ack = new CommitStreamAckPacket(stream_id, my_crc.getValue(), match);
            ctx.writeAndFlush(ack);
        } catch (IOException e) {
            LOGGER.error("unexpteced excetion when doing crc:" + this, e);
            ctx.writeAndFlush(new CommitStreamAckPacket(stream_id, 0, false));
        }
    }

    @Override
    public void decodePacket(ByteBuf from) {
        this.stream_id = UUIDCodec.decode(from);
        this.length = from.readLong();
        this.crc = from.readLong();
    }

    @Override
    public void encodePacket(ByteBuf to) {
        UUIDCodec.encdoe(to, stream_id) // uuid
                .writeLong(length) // lenth
                .writeLong(crc) // crc
        ;
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

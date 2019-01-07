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

public class CrcReportPacket implements Packet {

    public static final Log LOGGER = LogFactory.getLog(CrcReportPacket.class);

    protected UUID stream_id;
    protected long length;
    protected long crc;

    public CrcReportPacket(UUID stream_id, long length, long crc) {
        this.stream_id = stream_id;
        this.length = length;
        this.crc = crc;
    }

    protected CrcReportPacket() {
    }

    @Override
    public void decodeComplete(ChannelHandlerContext ctx) {
        try {
            LOGGER.info("do crc:" + this);
            CRC32 my_crc = new CRC32();
            my_crc.update(ChunkServent.mmap(stream_id, 0, length));

            CrcAckPacket ack = new CrcAckPacket(stream_id, my_crc.getValue() == crc);
            ctx.channel().writeAndFlush(ack);

            LOGGER.info("out crc ack:" + ack + " crc:" + my_crc.getValue());
        } catch (IOException e) {
            LOGGER.error("unexpteced excetion when doing crc:" + this, e);
            ctx.channel().writeAndFlush(new CrcAckPacket(stream_id, false));
        }
    }

    @Override
    public CrcReportPacket decodePacket(ByteBuf from) {
        UUID uuid = UUIDCodec.decode(from);
        long length = from.readLong();
        long crc = from.readLong();

        return new CrcReportPacket(uuid, length, crc);
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

package com.sf.misc.antman.simple.packets;

import com.sf.misc.antman.simple.Packet;
import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.UUID;

public class CommitStreamAckPacket implements Packet.NoAckPacket {

    public static final Log LOGGER = LogFactory.getLog(CommitStreamAckPacket.class);

    protected UUID stream_id;
    protected long crc;
    protected boolean match;

    public CommitStreamAckPacket(UUID stream_id, long crc, boolean match) {
        this.stream_id = stream_id;
        this.crc = crc;
        this.match = match;
    }

    protected CommitStreamAckPacket() {
    }

    @Override
    public void decodePacket(ByteBuf from) {
        this.stream_id = UUIDCodec.decode(from);
        this.crc = from.readLong();
        this.match = from.readByte() == 0x01 ? true : false;
    }

    @Override
    public void encodePacket(ByteBuf to) {
        UUIDCodec.encdoe(to, stream_id)
                .writeLong(crc)
                .writeByte(match ? 0x01 : 0x00);
    }

    @Override
    public byte type() {
        return 0x03;
    }

    @Override
    public String toString() {
        return "crc ack:" + this.stream_id + " match:" + match + " crc:" + crc;
    }
}

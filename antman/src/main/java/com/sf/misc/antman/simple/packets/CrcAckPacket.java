package com.sf.misc.antman.simple.packets;

import com.sf.misc.antman.simple.Packet;
import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.UUID;

public class CrcAckPacket implements Packet.NoAckPacket {

    public static final Log LOGGER = LogFactory.getLog(CrcAckPacket.class);

    protected UUID stream_id;
    protected boolean match;

    public CrcAckPacket(UUID stream_id, boolean match) {
        this.stream_id = stream_id;
        this.match = match;
    }

    protected CrcAckPacket() {
    }

    @Override
    public Packet decodePacket(ByteBuf from) {
        UUID uuid = UUIDCodec.decode(from);
        boolean match = from.readByte() == 0x01 ? true : false;

        return new CrcAckPacket(stream_id, match);
    }

    @Override
    public void encodePacket(ByteBuf to) {
        UUIDCodec.encdoe(to, stream_id)
                .writeByte(match ? 0x01 : 0x00);
    }

    @Override
    public byte type() {
        return 0x03;
    }

    @Override
    public String toString() {
        return "crc ack:" + this.stream_id + " match:" + match;
    }
}

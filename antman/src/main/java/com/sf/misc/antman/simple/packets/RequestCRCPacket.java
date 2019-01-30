package com.sf.misc.antman.simple.packets;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.UUID;

public class RequestCRCPacket implements Packet {

    public static final Log LOGGER = LogFactory.getLog(RequestCRCPacket.class);

    protected UUID stream;
    protected long offset;
    protected long length;

    public RequestCRCPacket(UUID uuid, long offset, long length) {
        this.stream = uuid;
        this.offset = offset;
        this.length = length;
    }

    protected RequestCRCPacket() {
    }

    public UUID getStream() {
        return stream;
    }

    public long getOffset() {
        return offset;
    }

    public long getLength() {
        return length;
    }

    @Override
    public void decodeComplete(ChannelHandlerContext ctx) {
        LOGGER.warn("not handle crc reqeust:" + this);
    }

    @Override
    public void decodePacket(ByteBuf from) {
        stream = UUIDCodec.decode(from);
        offset = from.readLong();
        length = from.readLong();
    }

    @Override
    public void encodePacket(ByteBuf to) {
        UUIDCodec.encdoe(to, stream)
                .writeLong(offset)
                .writeLong(length);
    }

    @Override
    public byte type() {
        return 0x04;
    }

    @Override
    public String toString() {
        return "[" + this.getClass().getSimpleName() + "]" + " stream:" + stream + " offset:" + offset + " length:" + length;
    }
}

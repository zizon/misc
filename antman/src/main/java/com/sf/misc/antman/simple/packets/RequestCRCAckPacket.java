package com.sf.misc.antman.simple.packets;

import com.sf.misc.antman.simple.Packet;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

import java.util.UUID;

public class RequestCRCAckPacket implements Packet.NoAckPacket {
    protected UUID stream;
    protected long offset;


    protected long length;
    protected long crc;

    public RequestCRCAckPacket(UUID uuid, long offset, long length, long crc) {
        this.stream = uuid;
        this.offset = offset;
        this.length = length;
        this.crc = crc;
    }

    protected RequestCRCAckPacket() {
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

    public long getCrc() {
        return crc;
    }


    @Override
    public void decodePacket(ByteBuf from) {
        stream = UUIDCodec.decode(from);
        offset = from.readLong();
        length = from.readLong();
        crc = from.readLong();
    }

    @Override
    public void encodePacket(ByteBuf to) {
        UUIDCodec.encdoe(to, stream)
                .writeLong(offset)
                .writeLong(length)
                .writeLong(crc);
    }

    @Override
    public byte type() {
        return 0x05;
    }
}

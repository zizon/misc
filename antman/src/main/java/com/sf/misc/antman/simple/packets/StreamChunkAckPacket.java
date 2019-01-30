package com.sf.misc.antman.simple.packets;

import io.netty.buffer.ByteBuf;

import java.util.UUID;

public class StreamChunkAckPacket implements Packet.NoAckPacket {

    protected UUID stream_id;
    protected long offset;
    protected long length;

    public StreamChunkAckPacket(UUID uuid, long offset, long length) {
        this.stream_id = uuid;
        this.offset = offset;
        this.length = length;
    }

    protected StreamChunkAckPacket() {
    }

    @Override
    public void decodePacket(ByteBuf from) {
        stream_id = UUIDCodec.decode(from);
        offset = from.readLong();
        length = from.readLong();
    }

    @Override
    public void encodePacket(ByteBuf to) {
        UUIDCodec.encdoe(to, stream_id)
                .writeLong(offset)
                .writeLong(length);
    }

    @Override
    public byte type() {
        return 0x04;
    }
}

package com.sf.misc.antman.simple.packets;

import io.netty.buffer.ByteBuf;

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
    public byte type() {
        return 0x06;
    }
}

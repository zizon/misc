package com.sf.misc.antman.simple.packets;

import java.util.UUID;

public class StreamChunkAckPacket implements Packet.NoAckPacket {

    @ProtocolField(order = 0)
    protected UUID stream_id;

    @ProtocolField(order = 1)
    protected long offset;

    @ProtocolField(order = 2)
    protected long length;

    public StreamChunkAckPacket(UUID uuid, long offset, long length) {
        this.stream_id = uuid;
        this.offset = offset;
        this.length = length;
    }

    protected StreamChunkAckPacket() {
    }

    @Override
    public byte type() {
        return 0x04;
    }
}

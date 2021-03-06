package com.sf.misc.antman.simple.packets;

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
    public byte type() {
        return 0x04;
    }
}

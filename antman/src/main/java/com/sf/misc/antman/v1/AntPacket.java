package com.sf.misc.antman.v1;

import io.netty.buffer.ByteBuf;

public class AntPacket implements AutoCloseable {

    protected final byte type;
    protected final ByteBuf buffer;

    public AntPacket(byte type, ByteBuf buffer) {
        this.buffer = buffer.retain();
        this.type = type;
    }

    public void close() throws Exception {
        this.buffer.release();
    }

    public byte type() {
        return this.type;
    }

    public ByteBuf zone() {
        return this.buffer.duplicate();
    }
}

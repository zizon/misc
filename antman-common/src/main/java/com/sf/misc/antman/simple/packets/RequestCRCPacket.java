package com.sf.misc.antman.simple.packets;

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
    public byte type() {
        return 0x05;
    }

    @Override
    public String toString() {
        return "[" + this.getClass().getSimpleName() + "]" + " stream:" + stream + " offset:" + offset + " length:" + length;
    }
}

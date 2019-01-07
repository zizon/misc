package com.sf.misc.antman.simple.packets;

import io.netty.buffer.ByteBuf;

import java.util.UUID;

public class UUIDCodec {

    public static ByteBuf encdoe(ByteBuf buf, UUID uuid) {
        buf.writeLong(uuid.getMostSignificantBits())
                .writeLong(uuid.getLeastSignificantBits());

        return buf;
    }

    public static UUID decode(ByteBuf buf) {
        long high = buf.readLong();
        long low = buf.readLong();
        return new UUID(high, low);
    }
}

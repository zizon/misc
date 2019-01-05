package com.sf.misc.antman.v1.processors;

import com.sf.misc.antman.io.ByteStream;
import com.sf.misc.antman.Promise;
import com.sf.misc.antman.v1.AntPacketProcessor;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.util.Optional;
import java.util.UUID;

public class StreamCrcAckProcessor implements AntPacketProcessor {

    @Override
    public byte type() {
        return 0x03;
    }

    @Override
    public Promise<Optional<ByteBuf>> process(ByteBuf input, ByteBufAllocator allocator) {
        return Promise.exceptional(() -> new RuntimeException("should not be invoke in server"));
    }

    public Promise<ByteBuf> ackCRC(ByteStream.StreamContext context, boolean ok, ByteBufAllocator allocator) {
        int expteced = Byte.BYTES // type
                + Long.BYTES + Long.BYTES  // uuid
                + Byte.BYTES;

        UUID uuid = context.uuid();
        ByteBuf output = allocator.ioBuffer(expteced);

        // type
        output.writeByte(this.type());

        // uuid
        output.writeLong(uuid.getMostSignificantBits());
        output.writeLong(uuid.getLeastSignificantBits());

        // ok or not
        output.writeByte(ok ? 0x1 : 0x0);

        return Promise.success(output);
    }
}

package com.sf.misc.antman.v1.processors;

import com.sf.misc.antman.io.ByteStream;
import com.sf.misc.antman.Promise;
import com.sf.misc.antman.v1.AntPacketProcessor;
import com.sf.misc.antman.v1.StreamContextLookup;
import com.sf.misc.antman.v1.StreamSink;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.util.Optional;
import java.util.UUID;
import java.util.zip.CRC32;

public class StreamReportProcessor implements AntPacketProcessor {

    @Override
    public byte type() {
        return 0x02;
    }

    @Override
    public Promise<Optional<ByteBuf>> process(ByteBuf input, ByteBufAllocator allocator) {
        // header
        long hign = input.readLong();
        long low = input.readLong();
        long length = input.readLong();
        long sender_crc = input.readLong();

        if (input.readableBytes() > 0) {
            return Promise.exceptional(() -> new IllegalStateException("buffer should consumed"));
        }

        UUID stream_id = new UUID(hign, low);
        return StreamContextLookup.ask(stream_id).transformAsync((context) -> {
            CRC32 crc = new CRC32();
            context.contents().sequential()
                    .forEach((promise) -> crc.update(promise.join()));

            long my = crc.getValue();
            if (my != sender_crc) {
                return crcNotMatch(context, allocator);
            }

            return crctMatch(context, my, length, allocator);
        }).transform((buffer) -> Optional.of(buffer));
    }

    protected Promise<ByteBuf> crcNotMatch(ByteStream.StreamContext context, ByteBufAllocator allocator) {
        return StreamSink.badCRC(context, allocator);
    }

    protected Promise<ByteBuf> crctMatch(ByteStream.StreamContext context, long crc, long length, ByteBufAllocator allocator) {
        return StreamSink.flush(context, crc, length, allocator);
    }
}

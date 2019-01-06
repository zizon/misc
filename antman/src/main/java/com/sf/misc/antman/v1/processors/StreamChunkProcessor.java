package com.sf.misc.antman.v1.processors;

import com.sf.misc.antman.io.ByteStream;
import com.sf.misc.antman.Promise;
import com.sf.misc.antman.v1.AntPacketProcessor;
import com.sf.misc.antman.v1.StreamContextLookup;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Optional;
import java.util.UUID;

public class StreamChunkProcessor implements AntPacketProcessor {

    public static final Log LOGGER = LogFactory.getLog(StreamChunkProcessor.class);

    protected final Promise<ByteStream> stream;

    public StreamChunkProcessor(Promise<ByteStream> stream) {
        this.stream = stream;
    }

    @Override
    public byte type() {
        return 0x01;
    }

    @Override
    public Promise<Optional<ByteBuf>> process(ByteBuf input, ByteBufAllocator allocator) {
        // header
        long hign = input.readLong();
        long low = input.readLong();
        long offset = input.readLong();
        long length = input.readLong();

        if (input.readableBytes() != length) {
            return Promise.exceptional(() ->
                    new IllegalStateException(
                            "packet length not match,input:" + input.readableBytes()
                                    + " detail" + input
                                    + " expected:" + length
                                    + " offset:" + offset
                                    + " uuid" + new UUID(hign, low)
                    )
            );
        }

        return stream.transformAsync((stream) -> {
            return StreamContextLookup.ask(new UUID(hign, low))
                    .transform((context) -> {
                        return new ByteStream.TransferReqeust(
                                context,
                                offset,
                                length,
                                (writable, request_offset) -> {
                                    // move to start
                                    //LOGGER.info("content:" + content + " writable:" + writable + " request:" + request_offset);
                                    ByteBuf slice = input.slice((int) request_offset, writable.remaining());
                                    slice.readBytes(writable);
                                    return Promise.success(null);
                                }
                        );
                    })
                    .transformAsync(stream::transfer);
        }).transform((ignore) -> {
            return Optional.empty();
        });
    }


}

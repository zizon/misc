package com.sf.misc.antman.v1;

import com.sf.misc.antman.Promise;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import javax.annotation.processing.Processor;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public interface AntPacketProcessor {

    public byte type();

    public Promise<Optional<ByteBuf>> process(ByteBuf input, ByteBufAllocator allocator);

    static final ConcurrentMap<Byte, AntPacketProcessor> PACKET_PROCESSOR = new ConcurrentHashMap<>();

    public static Promise<AntPacketProcessor> register(AntPacketProcessor processor) {
        if (PACKET_PROCESSOR.putIfAbsent(processor.type(), processor) != null) {
            return Promise.exceptional(() -> new IllegalStateException("duplicated antpacket:" + processor.type()));
        }

        return Promise.success(processor);
    }

    public static Promise<AntPacketProcessor> find(Class<? extends Processor> processor) {
        return PACKET_PROCESSOR.values().stream().filter((registed) -> registed.getClass() == processor)
                .findAny()
                .map((found) -> Promise.success(found))
                .orElseGet(() -> Promise.exceptional(() -> new RuntimeException("no processor register for:" + processor)));
    }

    public static Promise<AntPacketProcessor> find(byte type) {
        return PACKET_PROCESSOR.values().stream().filter((registed) -> registed.type() == type)
                .findAny()
                .map((found) -> Promise.success(found))
                .orElseGet(() -> Promise.exceptional(() -> new RuntimeException("no processor register for:" + type)));
    }
}

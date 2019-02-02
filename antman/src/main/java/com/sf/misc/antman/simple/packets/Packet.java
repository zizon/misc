package com.sf.misc.antman.simple.packets;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.sf.misc.antman.LightReflect;
import com.sf.misc.antman.Promise;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public interface Packet {

    static final Log LOGGER = LogFactory.getLog(Packet.class);

    static final byte VERSION = 0x01;

    static class Registry {
        protected final ConcurrentMap<Byte, Promise.PromiseSupplier<? extends Packet>> packets = new ConcurrentHashMap<>();

        public Registry register(Promise.PromiseSupplier<? extends Packet> packet) {
            return this.register(packet, false);
        }

        public Registry repalce(Promise.PromiseSupplier<? extends Packet> packet) {
            return this.register(packet, true);
        }

        protected Registry register(Promise.PromiseSupplier<? extends Packet> supplier, boolean replace) {
            Packet template = supplier.get();
            packets.compute(template.type(), (key, old) -> {
                if (old != null) {
                    if (replace) {
                        return supplier;
                    }

                    throw new IllegalStateException("packet:" + template.type() + " already registerd");
                }
                return supplier;
            });

            return this;
        }

        public Optional<Packet> guess(ByteBuf buf) {
            ByteBuf touch = buf.duplicate();
            long header = Integer.BYTES // packet length
                    + Byte.BYTES // version
                    + Byte.BYTES // type
                    ;

            // guess not possible
            if (touch.readableBytes() < header) {
                return Optional.empty();
            }

            // check packet length
            int packet_legnth = touch.readInt();
            if (touch.readableBytes() < packet_legnth) {
                return Optional.empty();
            }

            // gusess type
            touch.readByte();

            // find type
            byte type = touch.readByte();

            int slice_length = Integer.BYTES  // prefix
                    + packet_legnth // length
                    ;

            Packet packet = packets.get(type).get();
            packet.decode(buf.slice(buf.readerIndex(), slice_length));

            // fix buf at final
            // put it last to ensure buffer consistent.
            // since some delegated implementation may mis-behavior
            buf.readerIndex(buf.readerIndex() + slice_length);
            return Optional.of(packet);
        }
    }

    static interface NoAckPacket extends Packet {
        @Override
        default void decodeComplete(ChannelHandlerContext ctx) {
            // noop
        }
    }

    void decodeComplete(ChannelHandlerContext ctx);

    byte type();

    default void decodePacket(ByteBuf from) {
        PacketCodec.decode(this, from);
        return;
    }

    default void encodePacket(ByteBuf to) {
        PacketCodec.encode(this, to);
        return;
    }

    default void encode(ByteBuf to) {
        ByteBuf duplicate = to.duplicate();
        ByteBuf packet_content = this.skipHeader(to);

        long save = to.writerIndex();
        this.encodePacket(to);
        long length = to.writerIndex() - save;

        duplicate.writeInt((int) (Byte.BYTES // version
                + Byte.BYTES  // type
                + length) // content
        );
    }

    default ByteBuf skipHeader(ByteBuf to) {
        long header = Integer.BYTES // packet length
                + Byte.BYTES // version
                + Byte.BYTES // type
                ;

        to.writeInt(0); // packet length
        to.writeByte(VERSION); // version
        to.writeByte(this.type());  // type

        return to;
    }

    default void decode(ByteBuf from) {
        decodePacket(strip(from));
    }

    default ByteBuf strip(ByteBuf full) {
        long header = Integer.BYTES // packet length
                + Byte.BYTES // version
                + Byte.BYTES // type
                ;

        int length = full.readInt();
        if (length != full.readableBytes()) {
            throw new IllegalStateException("packet lenght not match:" + full.readableBytes() + " expected:" + length);
        }

        byte version = full.readByte();
        if (!this.versionCheck(version)) {
            throw new IllegalArgumentException("version not match:" + version);
        }

        byte type = full.readByte();
        if (type != this.type()) {
            throw new IllegalArgumentException("type not match:" + type + " expected:" + this.type());
        }

        return full;
    }

    default boolean versionCheck(byte version) {
        return version == VERSION;
    }
}

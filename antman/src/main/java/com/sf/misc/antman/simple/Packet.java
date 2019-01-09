package com.sf.misc.antman.simple;

import com.sf.misc.antman.Promise;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

public interface Packet {

    public static final Log LOGGER = LogFactory.getLog(Packet.class);

    public static final byte VERSION = 0x01;

    public static class Registry {
        protected final ConcurrentMap<Byte, Promise.PromiseSupplier<Packet>> packets = new ConcurrentHashMap<>();

        public Registry register(Promise.PromiseSupplier<Packet> packet) {
            return this.register(packet, false);
        }

        public Registry repalce(Promise.PromiseSupplier<Packet> packet) {
            return this.register(packet, true);
        }

        protected Registry register(Promise.PromiseSupplier<Packet> supplier, boolean replace) {
            Packet template = supplier.get();
            Promise.PromiseSupplier<?> old = packets.putIfAbsent(template.type(), supplier);
            if (old != null) {
                if (replace) {
                    Supplier<?> removed = packets.put(template.type(), supplier);
                    if (removed != null) {
                        LOGGER.info("update packet registery, remove:" + removed + " new:" + supplier + " old:" + old);
                    }
                    return this;
                }
                throw new IllegalStateException("packet:" + template.type() + " already registerd");
            }

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

    public static interface NoAckPacket extends Packet {
        @Override
        default public void decodeComplete(ChannelHandlerContext ctx) {
            // noop
        }
    }


    public void decodeComplete(ChannelHandlerContext ctx);

    public void decodePacket(ByteBuf from);

    public void encodePacket(ByteBuf to);

    public byte type();

    default public void encode(ByteBuf to) {
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

    default public ByteBuf skipHeader(ByteBuf to) {
        long header = Integer.BYTES // packet length
                + Byte.BYTES // version
                + Byte.BYTES // type
                ;

        to.writeInt(0); // packet length
        to.writeByte(VERSION); // version
        to.writeByte(this.type());  // type

        return to;
    }

    default public void decode(ByteBuf from) {
        decodePacket(strip(from));
    }

    default public ByteBuf strip(ByteBuf full) {
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

    default public boolean versionCheck(byte version) {
        return version == VERSION;
    }


}

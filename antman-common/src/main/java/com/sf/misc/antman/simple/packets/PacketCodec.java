package com.sf.misc.antman.simple.packets;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.sf.misc.antman.LightReflect;
import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public interface PacketCodec {

    public static Log LOGGER = LogFactory.getLog(PacketCodec.class);

    static final LoadingCache<Class<? extends Packet>, MethodHandle> ENCODERS = CacheBuilder.newBuilder().build(new CacheLoader<Class<? extends Packet>, MethodHandle>() {
        @Override
        public MethodHandle load(Class<? extends Packet> key) throws Exception {
            return createEncoder(key);
        }
    });

    static final LoadingCache<Class<? extends Packet>, MethodHandle> DECODERS = CacheBuilder.newBuilder().build(new CacheLoader<Class<? extends Packet>, MethodHandle>() {
        @Override
        public MethodHandle load(Class<? extends Packet> key) throws Exception {
            return createDecoder(key);
        }
    });

    static LightReflect LIGHT_REFLECT = LightReflect.create();

    Packet packet();

    public static void encode(Packet packet, ByteBuf to) {
        new PacketCodec() {
            @Override
            public Packet packet() {
                return packet;
            }
        }.encode(to);
    }

    public static void decode(Packet packet, ByteBuf from) {
        new PacketCodec() {
            @Override
            public Packet packet() {
                return packet;
            }
        }.decode(from);
    }


    static MethodHandle fieldEncdoer(Class<?> type) {
        Supplier<RuntimeException> exception_provider = () -> new RuntimeException("no encoder for type:" + type);
        LightReflect share = shareReflect();
        if (long.class.isAssignableFrom(type)) {
            return share.method(
                    ByteBuf.class,
                    "writeLong",
                    MethodType.methodType(ByteBuf.class, long.class)
            ).orElseThrow(exception_provider);
        } else if (boolean.class.isAssignableFrom(type)) {
            return share.method(
                    ByteBuf.class,
                    "writeBoolean",
                    MethodType.methodType(ByteBuf.class, boolean.class)
            ).orElseThrow(exception_provider);
        } else if (UUID.class.isAssignableFrom(type)) {
            return share.staticMethod(
                    PacketCodec.class,
                    "encodeUUID",
                    MethodType.methodType(
                            ByteBuf.class,
                            ByteBuf.class,
                            UUID.class
                    )
            ).orElseThrow(exception_provider);
        }

        throw exception_provider.get();
    }

    static MethodHandle fieldDecoders(Class<?> type) {
        Supplier<RuntimeException> exception_provider = () -> new RuntimeException("no encoder for type:" + type);
        LightReflect share = shareReflect();
        if (long.class.isAssignableFrom(type)) {
            return share.method(
                    ByteBuf.class,
                    "readLong",
                    MethodType.methodType(long.class)
            ).orElseThrow(exception_provider);
        } else if (boolean.class.isAssignableFrom(type)) {
            return share.method(
                    ByteBuf.class,
                    "readBoolean",
                    MethodType.methodType(boolean.class)
            ).orElseThrow(exception_provider);
        } else if (UUID.class.isAssignableFrom(type)) {
            return share.staticMethod(
                    PacketCodec.class,
                    "decodeUUID",
                    MethodType.methodType(UUID.class, ByteBuf.class)
            ).orElseThrow(exception_provider);
        }

        throw exception_provider.get();
    }

    static ByteBuf encodeUUID(ByteBuf to, UUID uuid) {
        return to.writeLong(uuid.getLeastSignificantBits())
                .writeLong(uuid.getMostSignificantBits());
    }

    static UUID decodeUUID(ByteBuf from) {
        long least = from.readLong();
        long most = from.readLong();
        return new UUID(most, least);
    }

    static MethodHandle createEncoder(Class<? extends Packet> type) {
        LightReflect share = shareReflect();
        // find getters
        MethodHandle[] getters = share.declearedGetters(type)
                .map(share::invokable)
                .toArray(MethodHandle[]::new);

        MethodHandle[] encoders = share.declaredFields(type).parallel()
                .map((field) -> {
                    return fieldEncdoer(field.getType());
                })
                .filter(Objects::nonNull)
                .map(share::invokable)
                .toArray(MethodHandle[]::new);

        MethodHandle call_encode = share.staticMethod(
                PacketCodec.class,
                "callEncode",
                MethodType.methodType(
                        void.class,
                        int.class,
                        MethodHandle[].class,
                        MethodHandle[].class,
                        Packet.class,
                        ByteBuf.class
                )
        ).orElseThrow(() -> new RuntimeException("no encoder handler found"));

        //call_encode.type().insertParameterTypes()
        int num_of_fields = getters.length;
        if (num_of_fields != encoders.length) {
            throw new RuntimeException("num of gettter and encoder not match for:" + type
                    + " ,getter:" + getters.length
                    + " encoders:" + encoders.length
                    + " fields:" + share.declaredFields(type)
                    .map((field) -> {
                        return "name:" + field.getName() + " type:" + field.getType();
                    })
                    .collect(Collectors.joining(",")));
        }

        // bind num of field
        call_encode = share.bind(call_encode, 0, num_of_fields);
        call_encode = share.bind(call_encode, 0, getters);
        call_encode = share.bind(call_encode, 0, encoders);

        return share.invokable(call_encode);
    }


    static void callEncode(int fields, MethodHandle[] getters, MethodHandle[] encdoers, Packet packet, ByteBuf to) {
        LightReflect share = shareReflect();
        for (int i = 0; i < fields; i++) {
            Object value = share.invoke(getters[i], packet);
            share.invoke(encdoers[i], to, value);
        }
    }

    static MethodHandle createDecoder(Class<? extends Packet> type) {
        LightReflect share = shareReflect();
        LOGGER.info("create decorder for:" + type + share.declaredFields(type).collect(Collectors.toList()));
        // find getters
        MethodHandle[] settters = share.declearedSetters(type)
                .map(share::invokable)
                .toArray(MethodHandle[]::new);

        MethodHandle[] decoders = share.declaredFields(type).parallel()
                .map((field) -> {
                    return fieldDecoders(field.getType());
                })
                .filter(Objects::nonNull)
                .map(share::invokable)
                .toArray(MethodHandle[]::new);

        MethodHandle call_decode = share.staticMethod(
                PacketCodec.class,
                "callDecode",
                MethodType.methodType(
                        void.class,
                        int.class,
                        MethodHandle[].class,
                        MethodHandle[].class,
                        Packet.class,
                        ByteBuf.class
                )
        ).orElseThrow(() -> new RuntimeException("no decoder handler found"));

        int num_of_fields = settters.length;
        if (num_of_fields != decoders.length) {
            throw new RuntimeException("num of gettter and encoder not match for:" + type
                    + " ,settter:" + settters.length
                    + " decoder:" + decoders.length
                    + " fields:" + share.declaredFields(type)
                    .map((field) -> {
                        return "name:" + field.getName() + " type:" + field.getType();
                    })
                    .collect(Collectors.joining(",")));
        }

        // bind num of field
        call_decode = share.bind(call_decode, 0, num_of_fields);
        call_decode = share.bind(call_decode, 0, settters);
        call_decode = share.bind(call_decode, 0, decoders);

        return share.invokable(call_decode);
    }

    static void callDecode(int fields, MethodHandle[] setters, MethodHandle[] decoders, Packet packet, ByteBuf from) {
        LightReflect share = shareReflect();
        for (int i = 0; i < fields; i++) {
            Object value = share.invoke(decoders[i], from);
            share.invoke(setters[i], packet, value);
        }
    }

    static LightReflect shareReflect() {
        return LIGHT_REFLECT;
    }

    default void encode(ByteBuf to) {
        Packet packet = packet();
        reflect().invoke(encoder(packet.getClass()), packet, to);
    }

    default void decode(ByteBuf from) {
        reflect().invoke(decoder(packet().getClass()), packet(), from);
    }

    default LightReflect reflect() {
        return shareReflect();
    }

    default MethodHandle encoder(Class<? extends Packet> packet_type) {
        return encoders().getUnchecked(packet_type);
    }

    default MethodHandle decoder(Class<? extends Packet> packet_type) {
        return decoders().getUnchecked(packet_type);
    }

    default LoadingCache<Class<? extends Packet>, MethodHandle> encoders() {
        return ENCODERS;
    }

    default LoadingCache<Class<? extends Packet>, MethodHandle> decoders() {
        return DECODERS;
    }
}

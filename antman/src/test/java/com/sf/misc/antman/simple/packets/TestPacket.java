package com.sf.misc.antman.simple.packets;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Test;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.UUID;

public class TestPacket {
    public static final Log LOGGER = LogFactory.getLog(TestPacket.class);

    public static class InnterPacket implements Packet.NoAckPacket {

        protected long some;
        protected UUID uuid;

        public InnterPacket(long some, UUID uuid) {
            this.some = some;
            this.uuid = uuid;
        }

        protected InnterPacket() {
        }

        @Override
        public byte type() {
            return 0;
        }
    }

    public static class CommitStreamAckPacketInherent extends CommitStreamAckPacket {

    }


    @Test
    public void testField() {
        Class<?> clazz = RequestCRCAckPacket.class;

        Arrays.stream(clazz.getDeclaredFields())
                .filter((field) -> !Modifier.isStatic(field.getModifiers()))
                .forEach((field) -> {
                    try {
                        MethodHandle getter = MethodHandles.lookup().findGetter(clazz, field.getName(), field.getType());

                        Object instance = clazz.newInstance();
                        LOGGER.info(getter.bindTo(instance).invoke());
                    } catch (Throwable e) {
                        LOGGER.error("fail to touch field:" + field, e);
                    }
                });
    }

    @Test
    public void testEncodeDecode() {
        long some = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        InnterPacket packet = new InnterPacket(some, uuid);
        ByteBuf buf = ByteBufAllocator.DEFAULT.heapBuffer();

        packet.encode(buf);

        InnterPacket deoced = new InnterPacket();
        deoced.decode(buf);

        Assert.assertEquals(some, deoced.some);
        Assert.assertEquals(uuid, deoced.uuid);

        LOGGER.info(deoced);
        LOGGER.info(packet);
    }

    @Test
    public void testInherent() {
        Arrays.stream(CommitStreamAckPacketInherent.class.getFields())
                //.filter((field) -> !Modifier.isStatic(field.getModifiers()))
                //.filter((field) -> !field.isSynthetic())
                .forEach((field) -> {
                    LOGGER.info(field);
                });

    }

}

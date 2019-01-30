package com.sf.misc.antman;

import com.sf.misc.antman.simple.packets.CommitStreamAckPacket;
import com.sf.misc.antman.simple.packets.Packet;
import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Test;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;


public class TestLightReflect {

    public static final Log LOGGER = LogFactory.getLog(TestLightReflect.class);

    public static int staticMehtodWithArgument(int base) {
        return base + 1;
    }


    public static class CommitStreamAckPacketInherent extends CommitStreamAckPacket {

    }


    @Test
    public void test() {
        try {
            MethodHandle origin = MethodHandles.lookup().findStatic(TestLightReflect.class, "staticMehtodWithArgument", MethodType.methodType(int.class, int.class));

            Object value = origin.invoke(10);
            Assert.assertEquals(11, value);

            // try drop argument
            MethodHandle rebond = MethodHandles.insertArguments(origin, 0, 11);
            value = rebond.invoke();
            Assert.assertEquals(12, value);
        } catch (Throwable throwable) {
            LOGGER.error("unexpected error", throwable);
        }
    }

    @Test
    public void testDeclearedFields() {
        LightReflect reflect = LightReflect.create();
        reflect.declaredFields(CommitStreamAckPacketInherent.class)
                .forEach((field) -> {
                    LOGGER.info(field);
                });

        reflect.declearedSetters(CommitStreamAckPacketInherent.class)
                .forEach((setter) -> {
                    LOGGER.info(setter);
                });
    }

    public static void InvokeTemplate(Packet packet, ByteBuf buf) {
    }

    @Test
    public void testInvokeAdaptor() {
        try {
            LightReflect reflect = LightReflect.create();
            MethodHandle handle = MethodHandles.lookup().findStatic(
                    TestLightReflect.class,
                    "InvokeTemplate",
                    MethodType.methodType(
                            void.class,
                            Packet.class,
                            ByteBuf.class
                    )
            ).asSpreader(Object[].class, 2).asType(MethodType.methodType(Object.class, Object[].class));


            reflect.invoke(handle, null, null);
        } catch (Throwable e) {
            LOGGER.error("unexpectd error", e);
            Assert.fail();
        }
    }
}

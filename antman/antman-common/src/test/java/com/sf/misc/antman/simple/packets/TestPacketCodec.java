package com.sf.misc.antman.simple.packets;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import java.util.Arrays;

public class TestPacketCodec {

    public static Log LOGGER = LogFactory.getLog(TestPacketCodec.class);

    @Test
    public void test(){
        PacketCodec.getProtocolFields(CommitStreamAckPacket.class).forEach((field)->{
            LOGGER.info(field);
        });

    }

}

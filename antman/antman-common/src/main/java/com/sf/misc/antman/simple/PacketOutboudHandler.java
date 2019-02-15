package com.sf.misc.antman.simple;

import com.sf.misc.antman.simple.packets.CommitStreamPacket;
import com.sf.misc.antman.simple.packets.Packet;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class PacketOutboudHandler extends MessageToByteEncoder<Packet> {

    public static final Log LOGGER = LogFactory.getLog(PacketOutboudHandler.class);

    @Override
    protected void encode(ChannelHandlerContext ctx, Packet msg, ByteBuf out) {
        try {
            msg.encode(out);
        } catch (Throwable throwable) {
            ctx.fireExceptionCaught(throwable);
        }
    }
}

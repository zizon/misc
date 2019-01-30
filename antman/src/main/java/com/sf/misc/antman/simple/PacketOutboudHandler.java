package com.sf.misc.antman.simple;

import com.sf.misc.antman.Promise;
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
        msg.encode(out);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
            throws Exception {
        LOGGER.error("unexpected exception:" + ctx.channel(), cause);
        Promise.wrap(ctx.close()).catching((throwable) -> {
            LOGGER.error("unexpected exception when trying to clean channel:" + ctx, throwable);
        });
    }
}

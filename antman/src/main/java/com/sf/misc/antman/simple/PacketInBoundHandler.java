package com.sf.misc.antman.simple;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Optional;

public class PacketInBoundHandler extends SimpleChannelInboundHandler<ByteBuf> {

    public static final Log LOGGER = LogFactory.getLog(PacketInBoundHandler.class);

    protected CompositeByteBuf gather;
    protected Packet.Registry registry;

    public PacketInBoundHandler(Packet.Registry registry) {
        this.registry = registry;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
        this.gather.addComponent(true, msg.retain());

        for (; ; ) {
            Optional<Packet> packet = registry.guess(this.gather);
            if (!packet.isPresent()) {
                break;
            }

            packet.get().decodeComplete(ctx);
        }

        // align compoments
        this.gather.discardReadComponents();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        this.gather = ctx.alloc().compositeBuffer().retain();
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        this.gather.release();
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
            throws Exception {
        LOGGER.error("unexpected exception", cause);
        ctx.close();
    }
}

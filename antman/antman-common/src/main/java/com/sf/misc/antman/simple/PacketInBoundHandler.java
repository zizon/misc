package com.sf.misc.antman.simple;

import com.sf.misc.antman.Streams;
import com.sf.misc.antman.simple.packets.Packet;
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
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
        try {
            // gather buffer
            this.gather.addComponent(true, msg.retain());

            // parse and process
            new Streams()
                    .generate(() -> {
                        return registry.guess(this.gather);
                    }).parallel()
                    .forEach((packet) -> packet.decodeComplete(ctx));

            // align compoments
            this.gather.discardReadComponents();
        } catch (Throwable throwable) {
            ctx.fireExceptionCaught(throwable);
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        this.gather = ctx.alloc().compositeBuffer().retain();
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        Optional.ofNullable(this.gather)
                .ifPresent((buffer) -> buffer.release());
        super.channelInactive(ctx);
    }
}

package com.sf.misc.antman.simple.server;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

public interface Packet {
    public void process(ChannelHandlerContext ctx, ByteBuf buf) throws Exception;
}

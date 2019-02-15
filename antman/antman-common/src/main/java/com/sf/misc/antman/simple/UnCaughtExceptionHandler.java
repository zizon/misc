package com.sf.misc.antman.simple;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class UnCaughtExceptionHandler extends ChannelInboundHandlerAdapter {

    public static final Log LOGGER = LogFactory.getLog(UnCaughtExceptionHandler.class);

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOGGER.error("unexpected exception, abort connection:" + ctx, cause);
        ctx.close();
    }
}

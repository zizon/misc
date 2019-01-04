package com.sf.misc.antman.v1;

import com.sf.misc.antman.FileStore;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.nio.charset.Charset;
import java.util.Optional;

public class AntServerHandler extends ChannelInboundHandlerAdapter {

    public static final Log LOGGER = LogFactory.getLog(AntServerHandler.class);
    public static final Charset CHARSET = Charset.forName("utf8");


    protected ChannelHandlerContext context;
    protected final FileStore.MMU mmu;


    public AntServerHandler(FileStore.MMU mmu) {
        this.mmu = Optional.of(mmu).get();
    }

    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        this.context = ctx;
        this.onRead((ByteBuf) msg);
    }

    protected void onRead(ByteBuf buf) {

        //TODO
    }
}

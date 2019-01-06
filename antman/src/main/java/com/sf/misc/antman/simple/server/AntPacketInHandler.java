package com.sf.misc.antman.simple.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.IOException;

public class AntPacketInHandler extends SimpleChannelInboundHandler<ByteBuf> {

    public static final Log LOGGER = LogFactory.getLog(AntPacketInHandler.class);

    public static final byte VERSION = 0x01;

    public static final byte STREAM_CHUNK_TYPE = 0x01;
    public static final byte STREAM_CRC_TYPE = 0x02;

    protected CompositeByteBuf gather;
    protected static StreamChunkPacket STREAM_CHUNK = new StreamChunkPacket(new File("__buckets__"));

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
        this.gather.addComponent(true, msg.retain());

        while (unpack(ctx)) {
        }

        this.gather.discardReadComponents();
    }

    protected boolean unpack(ChannelHandlerContext ctx) throws Exception {
        ByteBuf touch = this.gather.duplicate();
        if (touch.readableBytes() < Integer.BYTES) {
            return false;
        }

        // find packet length
        int packet_length = touch.readInt();
        if (touch.readableBytes() < packet_length) {
            return false;
        }

        // version
        byte version = touch.readByte();
        if (version != VERSION) {
            throw new IllegalStateException("version not match,expected:" + VERSION + " got:" + version + " packet lenght:" + packet_length + " gather:" + this.gather);
        }

        // type
        byte type = touch.readByte();

        // packet specifiy content
        int packet_header_length = Byte.BYTES // version
                + Byte.BYTES // type
                ;
        int content_length = packet_length - packet_header_length;

        // slice
        ByteBuf slice = touch.slice(touch.readerIndex(), content_length);

        // update read index
        this.gather.readerIndex((int) (this.gather.readerIndex()
                + Integer.BYTES // packet lenght
                + packet_length)
        );

        switch (type) {
            case STREAM_CHUNK_TYPE:
                STREAM_CHUNK.process(ctx, slice);
                return true;
            default:
                throw new IOException("unknow packet type");
        }
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
        System.exit(0);
    }
}

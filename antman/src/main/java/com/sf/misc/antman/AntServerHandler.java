package com.sf.misc.antman;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.internal.StringUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.nio.charset.Charset;
import java.util.UUID;

public class AntServerHandler extends ChannelInboundHandlerAdapter {

    public static final Log LOGGER = LogFactory.getLog(AntServerHandler.class);
    public static final Charset CHARSET = Charset.forName("utf8");

    public static final byte VERSION = 0x01;

    protected static enum State {
        VERSIONING,
        BLOCK_META,
        BLOCK_STREAM
    }

    protected State state;
    protected CompositeByteBuf buffer;
    protected HybridBlock block;

    public AntServerHandler() {
        this.state = State.VERSIONING;
        this.buffer = Unpooled.compositeBuffer();
    }

    protected void closeChannel(ChannelHandlerContext ctx) {
        // close
        Promise.submit(() -> ctx.close())//
                .sidekick((future) -> {
                    LOGGER.info("close channel:" + future.channel());
                }) //
                .catching((throwable) -> {
                    LOGGER.error("fail when closing channel:" + ctx.channel(), throwable);
                });
    }

    protected void dispatchState(ChannelHandlerContext ctx, ByteBuf buf) {
        // gather buffer if any
        if (buf != null) {
            buf.retain();
            this.buffer.addComponent(buf);
        }

        switch (state) {
            case VERSIONING:
                this.processVersioning(ctx);
                break;
            case BLOCK_META:
                this.processBlockMeta(ctx);
                break;
            case BLOCK_STREAM:
                this.processBlockStream(ctx);
                break;
            default:
                LOGGER.error("incomplete state maching:" + state + ", close channel");
                closeChannel(ctx);
                break;
        }
    }

    protected void processVersioning(ChannelHandlerContext ctx) {
        CompositeByteBuf local = this.buffer;
        ByteBuf duplicate = local.duplicate();

        // read version
        if (duplicate.readableBytes() < 1) {
            this.state = State.VERSIONING;
            return;
        }
        byte version = duplicate.readByte();

        // check version
        if (version != VERSION) {
            LOGGER.error("version mismatch,expected:" + StringUtil.byteToHexString(VERSION) + " but got:" + StringUtil.byteToHexString(version));
            this.closeChannel(ctx);
            return;
        }

        // read command
        if (duplicate.readableBytes() < Short.BYTES) {
            // not enough bytes
            this.state = State.VERSIONING;
            return;
        }
        short command = duplicate.readShort();

        // adjust buffer
        local.readerIndex(duplicate.readerIndex());
        local.discardReadBytes();

        // next state
        switch (command) {
            case 0:
                this.state = State.BLOCK_META;
                this.dispatchState(ctx, null);
                break;
            default:
                LOGGER.error("unknow command:" + command + " abort channel");
                this.closeChannel(ctx);
                break;
        }
    }

    protected void processBlockMeta(ChannelHandlerContext ctx) {
        if (block != null) {
            LOGGER.error("block should be null,abort channel");
            this.closeChannel(ctx);
            return;
        }

        CompositeByteBuf local = this.buffer;
        ByteBuf duplicated = local.duplicate();

        // block size
        if (duplicated.readableBytes() < Long.BYTES) {
            this.state = State.BLOCK_META;
            return;
        }
        int packet_size = duplicated.readableBytes();

        // file uuid
        if (duplicated.readableBytes() < 16) {
            this.state = State.BLOCK_META;
            return;
        }
        // read uuid
        UUID uuid = new UUID(duplicated.readLong(), duplicated.readLong());

        // read seq
        if (duplicated.readableBytes() < Long.BYTES) {
            this.state = State.BLOCK_META;
            return;
        }
        long seq = duplicated.readLong();

        int block_size = packet_size - (Long.BYTES + 16 + Long.BYTES);
        if (block_size < 0) {
            LOGGER.error("overflow block size:" + block_size);
            this.closeChannel(ctx);
            return;
        }

        // create block
        this.block = new HybridBlock(uuid, seq, block_size);

        // adjust
        local.readerIndex(duplicated.readerIndex());
        local.discardReadBytes();

        // flip state
        this.state = State.BLOCK_STREAM;
        this.dispatchState(ctx, null);
        return;
    }

    protected void processBlockStream(ChannelHandlerContext ctx) {
        HybridBlock block = this.block;
        CompositeByteBuf local = this.buffer;

        if (block.write(local)) {
            // finished
            this.state = State.VERSIONING;
            this.block = null;
            Promise.submit(block::commit) //
                    .sidekick(() -> {
                        commitBlock(ctx, block);
                    }) //
                    .sidekick(block::dispose) //
                    .catching((throwable) -> {
                        block.dispose();
                        LOGGER.error("fail when commit block:" + block, throwable);
                    });
        } else {
            this.state = State.BLOCK_STREAM;
        }

        local.discardReadBytes();
        if (local.readableBytes() > 0) {
            // continue processing
            dispatchState(ctx, null);
            return;
        }

        return;
    }

    protected void commitBlock(ChannelHandlerContext ctx, HybridBlock block) {
        ByteBuf buf = ctx.alloc().buffer(
                1 // version
                        + Short.BYTES // command commit
                        + 16 // uuid
                        + Long.BYTES // seq
                        + Integer.BYTES // crc
        );

        // setup buffer
        buf.writeByte(VERSION) // version
                .writeShort(0) // commit block
                .writeLong(block.uuid().getMostSignificantBits())  // uuid
                .writeLong(block.uuid().getLeastSignificantBits()) // uuid
                .writeLong(block.sequence()) // seq
                .writeInt(block.crc())
        ;

        Promise.wrap(ctx.writeAndFlush(buf))
                .sidekick((ignore) -> buf.release()) //
                .catching((exception) -> {
                    if (exception != null) {
                        LOGGER.warn("fail to commit block:" + block, exception);
                    }
                });
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        this.dispatchState(ctx, (ByteBuf) msg);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
            throws Exception {
        LOGGER.error("unexpected exception,close channel:" + ctx.channel(), cause);
        this.closeChannel(ctx);
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        LOGGER.info("channel:" + ctx.channel() + " close");

        // release buffer
        CompositeByteBuf local = this.buffer;
        if (local != null) {
            this.buffer = null;
            Promise.submit(() -> local.release()).logException();
        }

        // release block
        HybridBlock block = this.block;
        if (block != null) {
            this.block = null;
            Promise.submit(block::dispose).logException();
        }

        // forward
        super.channelUnregistered(ctx);
    }
}

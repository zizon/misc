package com.sf.misc.antman.v1;

import com.sf.misc.antman.io.FileStore;
import com.sf.misc.antman.Promise;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.AbstractMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

public class AntServerHandler extends ChannelInboundHandlerAdapter {

    public static final Log LOGGER = LogFactory.getLog(AntServerHandler.class);
    public static final Charset CHARSET = Charset.forName("utf8");

    public static final byte VERSION = 0x01;

    protected ChannelHandlerContext context;
    protected final FileStore.MMU mmu;
    protected CompositeByteBuf buffer;

    public AntServerHandler(FileStore.MMU mmu) {
        this.mmu = Optional.of(mmu).get();
    }

    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        this.context = ctx;
        this.onRead(ctx, (ByteBuf) msg);
    }

    protected Optional<Map.Entry<Byte, CompositeByteBuf>> parseOne(ByteBufAllocator allocator) throws IOException {
        // check if can build packet
        int lenght_prefix = Integer.BYTES;

        // touch
        if (this.buffer.readableBytes() < lenght_prefix) {
            return Optional.empty();
        }
        ByteBuf touch = this.buffer.duplicate();

        // full packet?
        int length = touch.readInt();
        if (touch.readableBytes() < length) {
            return Optional.empty();
        }

        byte version = touch.readByte();
        // check protocol version
        if (version != VERSION) {
            throw new IOException("protocol not match:" + version + " expected:" + VERSION);
        }

        byte type = touch.readByte();
        int header = Byte.BYTES // version
                + Byte.BYTES // packet type
                ;
        // cut header but not couting length field.
        // that means,lenght prefix is not include in couting
        int slice_length = length - header;

        CompositeByteBuf slice = this.sliceCompositeByteBuffer(this.buffer, touch.readerIndex(), slice_length, allocator);

        LOGGER.info("one slice");
        return Optional.of(new AbstractMap.SimpleImmutableEntry<>(type, slice));
    }

    protected CompositeByteBuf sliceCompositeByteBuffer(CompositeByteBuf buffer, int offset, long length, ByteBufAllocator allocator) {
        CompositeByteBuf slice = allocator.compositeBuffer();
        long end_offset = offset + length;
        while (offset < end_offset) {
            ByteBuf buf = buffer.componentAtOffset(offset);
            int expected_end_offset = offset + buf.readableBytes();
            if (expected_end_offset > end_offset) {
                buf.writerIndex((int) (buf.readerIndex() + end_offset - offset));
            }

            // retain
            slice.addComponent(true, buf.retain());

            offset += buf.readableBytes();
        }

        buffer.readerIndex((int) (buffer.readerIndex() + length));
        buffer.discardReadComponents();
        return slice;
    }

    protected void onRead(ChannelHandlerContext ctx, ByteBuf buf) throws Exception {
        this.buffer.addComponent(true, buf);
        for (; ; ) {
            Optional<Map.Entry<Byte, CompositeByteBuf>> parse_one = parseOne(ctx.alloc());
            if (!parse_one.isPresent()) {
                return;
            }

            byte type = parse_one.get().getKey();
            ByteBuf slice = parse_one.get().getValue();
            AntPacketProcessor.find(type)
                    .transformAsync((packet) -> {
                        return packet.process(slice, ctx.alloc())
                                .addListener(() -> slice.release());
                    })
                    .sidekick(() -> LOGGER.info("success full process a packet"))
                    .transformAsync((optional) -> {
                        if (optional.isPresent()) {
                            // touch output
                            ByteBuf output = optional.get();

                            int header_length = Byte.BYTES // version
                                    ;
                            int content_length = output.readableBytes();

                            int lenght_prefix = Integer.BYTES;

                            // prepare non_content_part
                            ByteBuf non_content_part = context.alloc().ioBuffer(lenght_prefix + header_length);
                            non_content_part.writeInt(header_length + content_length);
                            non_content_part.writeByte(VERSION);

                            // pack it
                            CompositeByteBuf final_out = context.alloc().compositeDirectBuffer();
                            LOGGER.info(final_out.refCnt());
                            System.exit(0);

                            // transfer ownership
                            final_out.addComponent(true, non_content_part);
                            final_out.addComponent(true, output);

                            return this.wrapFuture(ctx.writeAndFlush(final_out))
                                    .addListener(() -> final_out.release());
                        }

                        return Promise.success(null);
                    })
                    .catching((throwable) -> {
                        this.exceptionCaught(ctx, throwable);
                    });

        }


    }

    protected Promise<Void> wrapFuture(ChannelFuture future) {
        Promise<Void> promise = Promise.promise();
        future.addListener((value) -> {
            Throwable fail = value.cause();
            if (fail != null) {
                promise.completeExceptionally(fail);
                return;
            }

            try {
                value.get();
                promise.complete(null);
            } catch (Throwable throwable) {
                promise.completeExceptionally(throwable);
            }
        });

        return promise;
    }

    protected void closeChannel(ChannelHandlerContext ctx) {
        wrapFuture(ctx.close()).addListener(() -> LOGGER.info("channel:" + ctx + " closed"));
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        LOGGER.debug("active context:" + ctx);
        this.buffer = ctx.alloc().compositeBuffer();
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        this.buffer.release();
        this.buffer = null;
        super.channelInactive(ctx);
        System.exit(-1);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
            throws Exception {
        LOGGER.error("unexpected error,close", cause);
        this.closeChannel(ctx);
    }


}

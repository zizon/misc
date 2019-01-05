package com.sf.misc.antman.v1;

import com.sf.misc.antman.ClosableAware;
import com.sf.misc.antman.FileStore;
import com.sf.misc.antman.Promise;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.nio.charset.Charset;
import java.util.Optional;
import java.util.stream.Stream;

public class AntServerHandler extends ChannelInboundHandlerAdapter {

    public static final Log LOGGER = LogFactory.getLog(AntServerHandler.class);
    public static final Charset CHARSET = Charset.forName("utf8");


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

    protected void onRead(ChannelHandlerContext ctx, ByteBuf buf) {
        this.buffer.addComponent(buf.retain());

        // check if can build packet
        int expected = 1 // packet type
                + Short.BYTES // packet length
                ;

        // touch
        ByteBuf touch = this.buffer.duplicate();
        if (touch.readableBytes() <= expected) {
            return;
        }

        byte type = touch.readByte();
        Short length = touch.readShort();

        // accquire reference
        ByteBuf slice = touch.slice(touch.readerIndex(), length)
                .retain();

        // cut from buffer
        this.buffer.readerIndex(this.buffer.readerIndex() + expected + length);
        this.buffer.discardReadBytes();

        AntPacket packet = new AntPacket(type, slice);
        forward(packet, () -> ctx.alloc().ioBuffer())
                .transform((output) -> {
                    // retain reference
                    ByteBuf reference = output.retain();
                    ctx.writeAndFlush(reference).addListener((future) -> {
                        // release buffer
                        reference.release();

                        // notify if exception
                        if (!future.isSuccess()) {
                            this.exceptionCaught(ctx, future.cause());
                        }
                    });

                    return null;
                })
                // release buffer if done
                .sidekick((ignore) -> slice.release())
                .sidekick(packet::close)
                .catching((execption) -> {
                    slice.release();
                    ClosableAware.wrap(() -> packet).execute((ignore) -> {
                        this.exceptionCaught(ctx, execption);
                    });
                });
    }

    protected Promise<ByteBuf> forward(AntPacket packet, Promise.PromiseSupplier<ByteBuf> allocator) {
        //TODO
        switch (packet.type()) {
            //TODO
            default:
                return Promise.exceptional(() -> new RuntimeException("unregonized packet type:" + packet.type()));
        }

    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        this.buffer = ctx.alloc().compositeBuffer();
        super.channelActive(ctx);
    }

    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        this.buffer.release();
        this.buffer = null;
        super.channelInactive(ctx);
    }
}

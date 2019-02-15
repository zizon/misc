package com.sf.misc.antman.simple.client;

import com.sf.misc.antman.Promise;
import com.sf.misc.antman.simple.CRC;
import com.sf.misc.antman.simple.MemoryMapUnit;
import com.sf.misc.antman.simple.PacketInBoundHandler;
import com.sf.misc.antman.simple.PacketOutboudHandler;
import com.sf.misc.antman.simple.UnCaughtExceptionHandler;
import com.sf.misc.antman.simple.packets.CommitStreamAckPacket;
import com.sf.misc.antman.simple.packets.CommitStreamPacket;
import com.sf.misc.antman.simple.packets.Packet;
import com.sf.misc.antman.simple.packets.PacketRegistryAware;
import com.sf.misc.antman.simple.packets.StreamChunkAckPacket;
import com.sf.misc.antman.simple.packets.StreamChunkPacket;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CancellationException;

public class IOChannel {

    public static Log LOGGER = LogFactory.getLog(IOChannel.class);

    public static EventLoopGroup eventloop() {
        return SHARE_EVENT_LOOP;
    }

    protected final IOContext context;
    protected final Promise<RemoteCommit> remote_commit;

    protected static final EventLoopGroup SHARE_EVENT_LOOP = new NioEventLoopGroup();
    protected final Promise<Channel> current;

    protected static class RemoteCommit {
        protected final long crc;
        protected final boolean success;

        public RemoteCommit(long crc, boolean success) {
            this.crc = crc;
            this.success = success;
        }

        public long crc() {
            return crc;
        }

        public boolean isSuccess() {
            return success;
        }
    }

    public IOChannel(SocketAddress address, IOContext context) {
        this.remote_commit = Promise.promise();
        this.context = context;
        this.current = createChannel(address);
    }

    public Promise<?> upload() {
        // sent
        Promise<?> sent = send(context);

        // do crc
        Promise<?> done_crc = sent.transformAsync((ignore) -> doCRC());

        // final acked
        Promise<?> acked = Promise.all(done_crc, remote_commit).transform((ignore) -> {
            if (remote_commit.join().isSuccess()) {
                context.onSuccess();
            }

            // close channel
            return null;
        });

        // close channel
        acked.addListener(() -> {
            current.transform((channel) -> Promise.wrap(channel.close()))
                    .addListener(() -> context.onChannelComplete());
        });

        // ack fail?
        acked.catching((throwable) -> {
            if (!acked.isCancelled()) {
                context.onAbortChannel(throwable);
            }
        });

        return acked;
    }

    protected Promise<?> doCRC() {
        // local crc
        Promise<Long> local_crc = CRC.crc(context.file());

        // sent crc
        Promise<?> request_crc = local_crc.transformAsync((crc) -> {
            return this.write(new CommitStreamPacket(context.streamID(), context.file().length(), crc, context.clientID(), context.file().getName()));
        });

        // notify
        return Promise.all(local_crc, remote_commit, request_crc).transform((ignore) -> {
            long local_calcualted_crc = local_crc.join();
            long remote_calculated_crc = remote_commit.join().crc();
            boolean success = remote_commit.join().isSuccess();

            if (local_calcualted_crc != remote_calculated_crc) {
                context.onCRCNotMatch(local_crc.join(), remote_calculated_crc);
            }

            if (!success) {
                context.onCommitFail();
            }
            return null;
        });
    }

    protected Promise<?> send(IOContext context) {
        // write ok
        return context.pending().stream()
                .map((range) -> {
                    Promise<ByteBuffer> buffer = mmu().map(context.file(), range.offset(), range.size());
                    Promise<StreamChunkPacket> packet = buffer.transform((content) -> new StreamChunkPacket(context.streamID(), range.offset(), content));

                    // sent
                    Promise<?> sent = packet.transformAsync((content) -> this.write(content)).transform((ignore) -> null);

                    // clean up
                    Promise.all(buffer, sent).addListener(() -> {
                        mmu().unmap(buffer.join());
                    });

                    // add timeout
                    Promise<Boolean> with_ack_timeout = sent.transformAsync((ignore) -> {
                        // sent done,add going/ack timeout
                        Promise<Boolean> timeout = Promise.promise();

                        // register going
                        context.going(range, timeout);

                        return timeout.timeout(() -> {
                            return true;
                        }, context.timeout());
                    });

                    // listen
                    return with_ack_timeout.catching((throwable) -> {
                        if (!with_ack_timeout.isCancelled()) {
                            context.onFailure(range, throwable);
                        }
                    }).transform((timeouted) -> {
                        // not null indiate timeout
                        Optional.ofNullable(timeouted).ifPresent((ignore) -> {
                            context.onTimeout(range);
                        });

                        return null;
                    });
                })
                .collect(Promise.collector());
    }

    protected MemoryMapUnit mmu() {
        return MemoryMapUnit.shared();
    }

    protected Promise<?> write(Packet packet) {
        return current.transformAsync((channel) -> {
            return Promise.wrap(channel.writeAndFlush(packet));
        });
    }

    protected Promise<Channel> createChannel(SocketAddress address) {
        ChannelFuture future = new Bootstrap()
                .group(SHARE_EVENT_LOOP)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<NioSocketChannel>() {
                    @Override
                    protected void initChannel(NioSocketChannel ch) throws Exception {
                        ch.pipeline() //
                                .addLast(new PacketOutboudHandler())
                                .addLast(new PacketInBoundHandler(newRegistry()))
                                .addLast(new UnCaughtExceptionHandler() {
                                    @Override
                                    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                        remote_commit.completeExceptionally(cause);
                                        super.exceptionCaught(ctx, cause);
                                    }
                                })
                        ;
                    }
                })
                .validate()
                .connect(address);

        return Promise.wrap(future).transform((ignore) -> future.channel());
    }

    protected Packet.Registry newRegistry() {
        return new PacketRegistryAware() {
        }.initializeRegistry(new Packet.Registry())
                .repalce(() -> new StreamChunkAckPacket() {
                    @Override
                    public void decodeComplete(ChannelHandlerContext ctx) {
                        context.onCommit(new IOContext.Range(this.offset, this.length));
                    }
                })
                .repalce(() -> new CommitStreamAckPacket() {
                    @Override
                    public void decodeComplete(ChannelHandlerContext ctx) {
                        remote_commit.complete(new RemoteCommit(this.crc, this.commited));
                    }
                });
    }
}

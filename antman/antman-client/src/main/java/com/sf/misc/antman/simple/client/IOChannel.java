package com.sf.misc.antman.simple.client;

import com.sf.misc.antman.Promise;
import com.sf.misc.antman.simple.CRC;
import com.sf.misc.antman.simple.MemoryMapUnit;
import com.sf.misc.antman.simple.PacketInBoundHandler;
import com.sf.misc.antman.simple.PacketOutboudHandler;
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
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.CancellationException;

public class IOChannel {

    public static EventLoopGroup eventloop() {
        return SHARE_EVENT_LOOP;
    }

    protected final IOContext context;
    protected final Promise<Long> remote_crc;

    protected static final EventLoopGroup SHARE_EVENT_LOOP = new NioEventLoopGroup();
    protected final Promise<Channel> current;

    public IOChannel(SocketAddress address, IOContext context) {
        this.remote_crc = Promise.promise();
        this.context = context;
        this.current = createChannel(address);
    }

    public Promise<?> upload() {
        // sent
        Promise<?> sent = send(context);

        // do crc
        Promise<?> done_crc = sent.transformAsync((ignore) -> doCRC());

        Promise<?> done = Promise.promise();

        // clean up
        done_crc.sidekick(() -> {
            context.onChannelComplete();
        }).catching((throwable) -> {
            context.onAbortChannel(throwable);
        }).addListener(() -> {
            current.sidekick((channel) -> {
                channel.close();
            });
        }).addListener(() -> {
            done.complete(null);
        });

        return done;
    }

    protected Promise<?> doCRC() {
        // local crc
        Promise<Long> local_crc = CRC.crc(context.file());

        // sent crc
        Promise<?> request_crc = local_crc.transformAsync((crc) -> {
            return this.write(new CommitStreamPacket(context.streamID(), context.file().length(), crc));
        });

        // notify
        return Promise.all(local_crc, remote_crc).transform((ignore) -> {
            if (local_crc.join().equals(remote_crc.join())) {
                context.onSuccess();
            } else {
                context.onCRCNotMatch(local_crc.join(), remote_crc.join());
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
                    Promise<?> sent = packet.transformAsync((content) -> this.write(content));

                    // clean up
                    Promise.all(buffer, sent).addListener(() -> {
                        mmu().unmap(buffer.join());
                    });

                    // add timeout
                    Promise<Boolean> with_ack_timeout = sent.transformAsync((ignore) -> {
                        Promise<Boolean> timeout = Promise.promise();

                        // register going
                        context.going(range, timeout);

                        return timeout.timeout(() -> {
                            return true;
                        }, context.timeout());
                    });

                    // listen
                    return with_ack_timeout.catching((throwable) -> {
                        if (!(throwable instanceof CancellationException)) {
                            context.onFailure(range, throwable);
                        }
                    }).transform((timeouted) -> {
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
                                .addLast(new PacketInBoundHandler(newRegistry()) {
                                    @Override
                                    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                        LOGGER.error("unexpected exception:" + ctx, cause);
                                    }
                                });
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
                        remote_crc.complete(this.crc);
                    }
                });
    }
}

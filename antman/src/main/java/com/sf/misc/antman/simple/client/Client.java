package com.sf.misc.antman.simple.client;

import com.sf.misc.antman.Promise;
import com.sf.misc.antman.simple.PacketInBoundHandler;
import com.sf.misc.antman.simple.PacketOutboudHandler;
import com.sf.misc.antman.simple.packets.CommitStreamAckPacket;
import com.sf.misc.antman.simple.packets.Packet;
import com.sf.misc.antman.simple.packets.PacketRegistryAware;
import com.sf.misc.antman.simple.packets.StreamChunkAckPacket;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.io.File;
import java.net.SocketAddress;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Client {

    public static interface SessionListener extends IOContext.ProgressListener {

    }

    public static class Session {

        protected IOContext context;
        protected IOChannel current;
        protected Promise.PromiseSupplier<SocketAddress> server_provider;
        protected BlockingQueue<Optional<IOContext.Range>> acked = new LinkedBlockingQueue<>();
        protected BlockingQueue<Long> acked_crc = new LinkedBlockingQueue<>();

        public Session(File file, UUID stream_id, Promise.PromiseSupplier<SocketAddress> server_provider, IOContext.ProgressListener listener, TuningParameters parameters) {
            this.context = new IOContext(
                    file,
                    stream_id,
                    parameters.chunk(),
                    parameters.chunk() / parameters.netIOBytesPerSecond() + parameters.driftDelay(),
                    listener
            );
            this.server_provider = server_provider;
            this.current = createHandler();
        }

        protected IOChannel createHandler() {
            return new IOChannel(new IOHandler() {
                Promise<Channel> channel = createChannel(server_provider.get());

                @Override
                public Promise<?> connect() {
                    return channel;
                }

                @Override
                public Promise<?> write(Packet packet) {
                    return channel.transformAsync((channel) -> {
                        return Promise.wrap(channel.writeAndFlush(packet));
                    });
                }

                @Override
                public Optional<IOContext.Range> read() {
                    try {
                        return acked.take();
                    } catch (InterruptedException e) {
                        throw new RuntimeException("interupted", e);
                    }
                }

                @Override
                public long readCRC() {
                    return 0;
                }

                @Override
                public void close() {
                    this
                }
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
                            acked.offer(Optional.of(new IOContext.Range(this.offset, this.length)));
                        }
                    })
                    .repalce(() -> new CommitStreamAckPacket() {
                        @Override
                        public void decodeComplete(ChannelHandlerContext ctx) {
                            acked_crc.offer(crc);
                        }
                    });
        }

    }

    public static EventLoopGroup eventloop() {
        return SHARE_EVENT_LOOP;
    }

    protected static final EventLoopGroup SHARE_EVENT_LOOP = new NioEventLoopGroup();

    protected final SocketAddress server;

    public Client(SocketAddress server) {
        this.server = server;
    }

    public Session upload(File file, UUID stream_id, SessionListener listener, TuningParameters parameters) {
        return new Session(file, stream_id, this::candidateServer, listener, parameters);
    }

    public SocketAddress candidateServer() {
        return server;
    }
}

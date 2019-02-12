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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.net.SocketAddress;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ClientIO {

    public static final Log LOGGER = LogFactory.getLog(ClientIO.class);

    protected static final EventLoopGroup SHARE_EVENT_LOOP = new NioEventLoopGroup();

    public static EventLoopGroup eventloop() {
        return SHARE_EVENT_LOOP;
    }

    public UploadChannel connect(SocketAddress address) {
        return new UploadChannel(new IOHandler() {
            BlockingQueue<Optional<IOContext.Range>> acked = new LinkedBlockingQueue<>();
            BlockingQueue<Long> acked_crc = new LinkedBlockingQueue<>();

            Promise<Channel> connected = createChannel();

            @Override
            public Promise<?> connect() {
                return connected;
            }

            protected Promise<Channel> createChannel() {
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

            @Override
            public Promise<?> write(Packet packet) {
                return connected.transformAsync(
                        (channel) -> Promise.wrap(channel.writeAndFlush(packet))
                );
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
                try {
                    return acked_crc.take();
                } catch (InterruptedException e) {
                    throw new RuntimeException("interupted", e);
                }
            }

            @Override
            public void close() {
                connected.transform((channel) -> channel.close())
                        .logException();
            }
        });
    }
}

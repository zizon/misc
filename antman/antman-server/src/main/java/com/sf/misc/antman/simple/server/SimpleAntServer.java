package com.sf.misc.antman.simple.server;

import com.sf.misc.antman.Promise;
import com.sf.misc.antman.simple.PacketInBoundHandler;
import com.sf.misc.antman.simple.PacketOutboudHandler;
import com.sf.misc.antman.simple.UnCaughtExceptionHandler;
import com.sf.misc.antman.simple.packets.CommitStreamAckPacket;
import com.sf.misc.antman.simple.packets.CommitStreamPacket;
import com.sf.misc.antman.simple.packets.Packet;
import com.sf.misc.antman.simple.packets.PacketCodec;
import com.sf.misc.antman.simple.packets.PacketRegistryAware;
import com.sf.misc.antman.simple.packets.StreamChunkAckPacket;
import com.sf.misc.antman.simple.packets.StreamChunkPacket;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.zip.CRC32;

public interface SimpleAntServer extends AutoCloseable {

    static final Log LOGGER = LogFactory.getLog(SimpleAntServer.class);

    public static Promise<SimpleAntServer> create(SocketAddress address) {
        return new SimpleAntServer() {
            Channel channel;

            @Override
            public ServerBootstrap bootstrap() {
                return new ServerBootstrap()
                        .group(new NioEventLoopGroup())
                        .option(ChannelOption.SO_REUSEADDR, true)
                        .option(ChannelOption.SO_BACKLOG, 30000)
                        ;
            }

            @Override
            public void channel(Channel channel) {
                this.channel = channel;
            }

            @Override
            public Channel channel() {
                return channel;
            }

            @Override
            public Packet.Registry registry() {
                return new PacketRegistryAware() {
                }.initializeRegistry(new Packet.Registry())
                        .repalce(() -> new StreamChunkPacket() {
                            @Override
                            public void decodeComplete(ChannelHandlerContext ctx) {
                                Promise.wrap(
                                        ctx.writeAndFlush(
                                                new StreamChunkAckPacket(
                                                        stream_id,
                                                        stream_offset,
                                                        content.remaining())
                                        )
                                ).addListener(() -> {
                                    ChunkServent.unmap(content).logException();
                                }).catching(ctx::fireExceptionCaught);
                            }

                            @Override
                            public void decodePacket(ByteBuf from) {
                                long header = Long.BYTES + Long.BYTES  // uuid
                                        + Long.BYTES // offset
                                        + Long.BYTES // stream length
                                        ;

                                this.stream_id = PacketCodec.decodeUUID(from);
                                this.stream_offset = from.readLong();
                                long chunk_length = from.readLong();

                                if (from.readableBytes() != chunk_length) {
                                    throw new RuntimeException("lenght not match,expect:" + chunk_length + " got:" + from.readableBytes() + " from:" + from + " uuid:" + stream_id + " offset:" + stream_offset);
                                }

                                ByteBuffer source = ChunkServent.mmap(stream_id, stream_offset, chunk_length).join();
                                from.readBytes(source.duplicate());

                                this.content = source;
                            }
                        })
                        .repalce(() -> new CommitStreamPacket() {
                            @Override
                            public void decodeComplete(ChannelHandlerContext ctx) {
                                // fetch page
                                Promise<ByteBuffer> fetching_page = ChunkServent.mmap(stream_id, 0, length);

                                // calculate crc
                                Promise<Long> my_crc = fetching_page.transform((page) -> {
                                    CRC32 crc = new CRC32();
                                    crc.update(page);
                                    LOGGER.info(page);
                                    return crc.getValue();
                                });

                                // unmap when crc calculated
                                Promise.all(fetching_page, my_crc)
                                        .addListener(
                                                () -> ChunkServent.unmap(fetching_page.join())
                                                        .logException()
                                        );

                                // match?
                                Promise<Boolean> commited = my_crc.transformAsync((local_crc) -> {
                                    if (local_crc != crc) {
                                        return Promise.success(false);
                                    }

                                    return ChunkServent.commit(stream_id, client_id).transform((ignore) -> true);
                                });

                                // send response
                                Promise.all(my_crc, commited).transformAsync((ignore) -> {
                                    long calculated_crc = my_crc.join();
                                    boolean commit = commited.join();
                                    boolean match = calculated_crc == crc && commit;
                                    LOGGER.info("stream:" + stream_id + " my crc:" + calculated_crc + " client crc:" + crc + " length:" + length + " file:" + ChunkServent.file(stream_id).length() + " commit:" + commit);

                                    return Promise.wrap(
                                            ctx.writeAndFlush(
                                                    new CommitStreamAckPacket(
                                                            stream_id,
                                                            calculated_crc,
                                                            match
                                                    ))
                                    );
                                }).catching(ctx::fireExceptionCaught);
                            }
                        })
                        ;
            }
        }.bind(address);
    }

    default Promise<SimpleAntServer> bind(SocketAddress address) {
        // try bind
        ChannelFuture future = bootstrap()
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<NioSocketChannel>() {
                    @Override
                    protected void initChannel(NioSocketChannel ch) throws Exception {
                        LOGGER.info("setup channel:" + ch);
                        pipeline(ch.pipeline());
                        return;
                    }
                })
                .validate()
                .bind(address);

        // seal channel
        channel(future.channel());

        // bond
        Promise<?> bond = Promise.wrap(future);

        // seal channel
        return bond.transform((ignore) -> this);
    }

    default ChannelPipeline pipeline(ChannelPipeline pipeline) {
        return pipeline.addLast(new PacketOutboudHandler())
                .addLast(new PacketInBoundHandler(registry()))
                // input encode
                .addLast(new ChannelInboundHandlerAdapter())
                .addLast(new UnCaughtExceptionHandler())
                ;
    }

    default void close() throws Exception {
        Optional.ofNullable(channel()).ifPresent(Channel::close);
    }

    ServerBootstrap bootstrap();

    void channel(Channel channel);

    Channel channel();

    Packet.Registry registry();


}

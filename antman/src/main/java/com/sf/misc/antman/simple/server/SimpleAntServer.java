package com.sf.misc.antman.simple.server;

import com.sf.misc.antman.Promise;
import com.sf.misc.antman.simple.PacketInBoundHandler;
import com.sf.misc.antman.simple.PacketOutboudHandler;
import com.sf.misc.antman.simple.packets.Packet;
import com.sf.misc.antman.simple.packets.PacketRegistryAware;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.net.SocketAddress;
import java.util.Optional;

public interface SimpleAntServer extends AutoCloseable {

    static final Log LOGGER = LogFactory.getLog(SimpleAntServer.class);

    public static Promise<SimpleAntServer> create(SocketAddress address) {
        Promise<?> closed = Promise.promise();
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
            public Promise<?> onClose() {
                return Optional.ofNullable(channel())
                        .map((channel) -> {
                            return Promise.wrap(channel.closeFuture());
                        }) //
                        .orElse(Promise.success(null))
                        .addListener(() -> closed.complete(null));
            }

            @Override
            public Packet.Registry registry() {
                return new PacketRegistryAware() {
                }.initializeRegistry(new Packet.Registry());
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
                .addLast(logging())
                ;
    }

    default void close() throws Exception {
        Optional.ofNullable(channel()).ifPresent(Channel::close);
    }

    default LoggingHandler logging() {
        return new LoggingHandler(LogLevel.INFO);
    }

    ServerBootstrap bootstrap();

    void channel(Channel channel);

    Channel channel();

    Promise<?> onClose();

    Packet.Registry registry();


}

package com.sf.misc.antman.simple.server;

import com.sf.misc.antman.simple.BootstrapAware;
import com.sf.misc.antman.simple.Packet;
import com.sf.misc.antman.simple.PacketInBoundHandler;
import com.sf.misc.antman.simple.PacketOutboudHandler;
import com.sf.misc.antman.simple.packets.PacketReigstryAware;
import io.netty.bootstrap.ServerBootstrap;
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

public class SimpleAntServer implements PacketReigstryAware, BootstrapAware {

    public static final Log LOGGER = LogFactory.getLog(SimpleAntServer.class);

    protected final ChannelFuture bind;

    public SimpleAntServer(SocketAddress address) {
        Packet.Registry registry = postInitializeRegistry(
                initializeRegistry(new Packet.Registry())
        );

        this.bind = bootstrap(new ServerBootstrap())
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<NioSocketChannel>() {
                    @Override
                    protected void initChannel(NioSocketChannel ch) throws Exception {
                        LOGGER.info("create channel:" + ch);
                        pipeline(ch.pipeline()) //
                                .addLast(new ChannelInboundHandlerAdapter() {
                                    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
                                            throws Exception {
                                        LOGGER.error("uncaucht exception,close channel:" + ctx.channel(), cause);
                                        ctx.channel().close();
                                    }
                                })
                                .addLast(new PacketInBoundHandler(registry))
                                .addLast(new PacketOutboudHandler())

                        ;
                        return;
                    }
                })
                .validate()
                .bind(address);
    }

    public ChannelFuture bind() {
        return this.bind.addListener((ignore) -> LOGGER.info("bind"));
    }

    protected ChannelPipeline pipeline(ChannelPipeline pipeline) {
        return pipeline;
    }
}

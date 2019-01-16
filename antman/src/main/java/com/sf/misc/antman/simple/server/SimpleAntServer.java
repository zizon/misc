package com.sf.misc.antman.simple.server;

import com.sf.misc.antman.Promise;
import com.sf.misc.antman.simple.BootstrapAware;
import com.sf.misc.antman.simple.Packet;
import com.sf.misc.antman.simple.PacketInBoundHandler;
import com.sf.misc.antman.simple.PacketOutboudHandler;
import com.sf.misc.antman.simple.packets.PacketReigstryAware;
import io.netty.bootstrap.AbstractBootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.net.SocketAddress;

public class SimpleAntServer implements PacketReigstryAware, BootstrapAware<ServerBootstrap> {

    public static final Log LOGGER = LogFactory.getLog(SimpleAntServer.class);

    protected final Promise<Channel> channel;

    public SimpleAntServer(SocketAddress address) {
        Packet.Registry registry = initializeRegistry(new Packet.Registry());

        ChannelFuture bond = bootstrap(new ServerBootstrap())
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<NioSocketChannel>() {
                    @Override
                    protected void initChannel(NioSocketChannel ch) throws Exception {
                        LOGGER.info("create channel:" + ch);
                        pipeline(ch.pipeline()) //
                                // output encode
                                .addLast(new PacketOutboudHandler())
                                .addLast(new PacketInBoundHandler(registry))
                                // input encode
                                .addLast(new ChannelInboundHandlerAdapter() {
                                    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
                                            throws Exception {
                                        LOGGER.error("uncaucht exception,close channel:" + ctx.channel(), cause);
                                        ctx.channel().close();
                                    }
                                })


                        ;
                        return;
                    }
                })
                .validate()
                .bind(address);

        // hould channel
        this.channel = Promise.wrap(bond).transform((ignore) -> bond.channel())
                .sidekick(() -> LOGGER.info("bind:" + address));

        // log when closd
        this.channel.transform((channel) -> Promise.wrap(channel.closeFuture()))
                .addListener(() -> LOGGER.info("channel:" + address + " closed"));
    }

    public Promise<SimpleAntServer> bind() {
        return this.channel.transform((ignore) -> this);
    }

    public Promise<SimpleAntServer> close() {
        return this.channel.transformAsync((channel) -> Promise.wrap(channel.close()))
                .transform((ignore) -> this);
    }


    protected ChannelPipeline pipeline(ChannelPipeline pipeline) {
        return pipeline;
    }
}

package com.sf.misc.antman.simple.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.net.SocketAddress;

public class SimpleAntServer {

    public static final Log LOGGER = LogFactory.getLog(SimpleAntServer.class);

    protected final ChannelFuture bind;

    public SimpleAntServer(SocketAddress address) {
        this.bind = new ServerBootstrap()
                .group(new NioEventLoopGroup())
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childHandler(new ChannelInitializer<NioSocketChannel>() {
                    @Override
                    protected void initChannel(NioSocketChannel ch) throws Exception {
                        LOGGER.info("create channel:" + ch);
                        ch.pipeline() //
                                .addLast(new AntPacketInHandler()) //
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
}

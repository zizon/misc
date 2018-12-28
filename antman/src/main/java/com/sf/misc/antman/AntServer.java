package com.sf.misc.antman;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

public class AntServer {
    public static final Log LOGGER = LogFactory.getLog(AntServer.class);

    public static void main(String args[]) {

        SocketAddress address = new InetSocketAddress(10086);
        LOGGER.info("try bind:" + address);
        Promise.wrap(new ServerBootstrap()
                .group(new NioEventLoopGroup())
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childHandler(new ChannelInitializer<NioSocketChannel>() {
                    @Override
                    protected void initChannel(NioSocketChannel ch) throws Exception {
                        ch.pipeline().addLast(new AntServerHandler());
                        return;
                    }
                })
                .validate()
                .bind(address)
        ).sidekick((ignore) -> {
            LOGGER.info("bind");
        }).catching((exception) -> {
            LOGGER.error("fail to bind", exception);
        });
    }
}

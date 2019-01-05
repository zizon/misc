package com.sf.misc.antman;

import com.sf.misc.antman.io.ByteStream;
import com.sf.misc.antman.io.FileStore;
import com.sf.misc.antman.v1.AntPacketProcessor;
import com.sf.misc.antman.v1.AntServerHandler;
import com.sf.misc.antman.v1.StreamContextLookup;
import com.sf.misc.antman.v1.processors.StreamChunkProcessor;
import com.sf.misc.antman.v1.processors.StreamCrcAckProcessor;
import com.sf.misc.antman.v1.processors.StreamReportProcessor;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

public class AntServer {
    public static final Log LOGGER = LogFactory.getLog(AntServer.class);

    protected final Promise<FileStore.MMU> mmu;

    public AntServer(File storage) {
        this.mmu = new FileStore(storage).mmu()
                .sidekick(() -> LOGGER.info("mmu initialized"))
                .transformAsync((mmu) -> {
                    return StreamContextLookup.startup(mmu)
                            .transform((ignore) -> mmu)
                            .sidekick(() -> LOGGER.info("stream context lookup started"));
                })
                .transformAsync((mmu) -> {
                    return registerPacketProcessors(Promise.success(new ByteStream(mmu)))
                            .transform((ignore) -> mmu) //
                            .sidekick(() -> LOGGER.info("packet processor registerd"));
                });
    }

    public Promise<Void> server(SocketAddress address) {
        return this.mmu.transformAsync((mmu) -> {
            LOGGER.info("start bind server at:" + address);
            return Promise.wrap(
                    new ServerBootstrap()
                            .group(new NioEventLoopGroup())
                            .channel(NioServerSocketChannel.class)
                            .option(ChannelOption.SO_REUSEADDR, true)
                            .childHandler(new ChannelInitializer<NioSocketChannel>() {
                                @Override
                                protected void initChannel(NioSocketChannel ch) throws Exception {
                                    ch.pipeline().addLast(new AntServerHandler(mmu));
                                    return;
                                }
                            })
                            .validate()
                            .bind(address)
            );
        });
    }

    protected Promise<Void> registerPacketProcessors(Promise<ByteStream> stream) {
        return Promise.light(() -> {
            LOGGER.info("reigster packet processors...");
            AntPacketProcessor.register(new StreamChunkProcessor(stream));
            AntPacketProcessor.register(new StreamCrcAckProcessor());
            AntPacketProcessor.register(new StreamReportProcessor());
            return null;
        });
    }
}


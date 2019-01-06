package com.sf.misc.antman.simple.client;

import com.sf.misc.antman.Promise;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.SocketAddress;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.zip.CRC32;

public class SimpleAntClient {

    public static final Log LOGGER = LogFactory.getLog(SimpleAntClient.class);

    protected final ChannelFuture connect;

    public SimpleAntClient(SocketAddress address) {
        this.connect = new Bootstrap()
                .group(new NioEventLoopGroup())
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline() //
                                .addLast(new MmapChunkHandler())
                                .addLast(new CrcReportHandler())
                        ;
                        return;
                    }
                })
                .validate()
                .connect(address);
    }

    public Promise<Void> uploadFile(File file) {
        return Promise.costly(() -> uploadFile(file, 4 * 1024)).transform((ignore) -> null);
    }

    protected Promise<Void> uploadFile(File file, long chunk_size) throws Exception {
        UUID uuid = UUID.nameUUIDFromBytes((file.toURI().toString() + file.length()).getBytes());

        FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.READ);

        // how many chunks
        long chunks = file.length() / chunk_size
                + (file.length() % chunk_size != 0 ? 1 : 0);

        long max = file.length();
        return Promise.costly(() -> {
            LOGGER.info("start upload file:" + file);
            for (ChannelFuture futre : LongStream.range(0, chunks).parallel()
                    .mapToObj((chunk_id) -> {
                        long absolute_offset = chunk_id * chunk_size;
                        long expected_size = chunk_size;
                        if (absolute_offset + expected_size > max) {
                            expected_size = max - absolute_offset;
                        }

                        try {
                            SteramChunk chunk = new SteramChunk(uuid, absolute_offset, channel.map(FileChannel.MapMode.READ_ONLY, absolute_offset, expected_size));
                            return this.connect.channel().writeAndFlush(chunk);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }).collect(Collectors.toList())) {
                futre.get();
            }
            LOGGER.info("transfer:" + file.toURI() + " done, uuid:" + uuid);

            CRC32 crc = new CRC32();
            crc.update(channel.map(FileChannel.MapMode.READ_ONLY, 0, max));

            LOGGER.info("file:" + file.toURI() + " uuid: " + uuid + " crc:" + crc.getValue());
            this.connect.channel().writeAndFlush(new StreamCrc(uuid, crc.getValue(), max)).get();
            this.connect.channel().close();
            return null;
        });

    }
}

package com.sf.misc.antman.simple.client;

import com.sf.misc.antman.Promise;
import com.sf.misc.antman.simple.BootstrapAware;
import com.sf.misc.antman.simple.Packet;
import com.sf.misc.antman.simple.PacketInBoundHandler;
import com.sf.misc.antman.simple.PacketOutboudHandler;
import com.sf.misc.antman.simple.packets.CrcAckPacket;
import com.sf.misc.antman.simple.packets.CrcReportPacket;
import com.sf.misc.antman.simple.packets.PacketReigstryAware;
import com.sf.misc.antman.simple.packets.StreamChunkPacket;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.FileRegion;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.SocketAddress;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.UUID;
import java.util.stream.LongStream;
import java.util.zip.CRC32;

public class SimpleAntClient implements PacketReigstryAware, BootstrapAware {

    public static final Log LOGGER = LogFactory.getLog(SimpleAntClient.class);

    protected final Promise.PromiseSupplier<ChannelFuture> connection_provider;

    public SimpleAntClient(SocketAddress address) {
        Packet.Registry registry = postInitializeRegistry(
                initializeRegistry(new Packet.Registry())
                        .repalce(new StreamChunkPacket(null, 0, null) {
                            @Override
                            public Packet decode(ByteBuf from) {
                                throw new UnsupportedOperationException("client should not call this");
                            }
                        })
                        .repalce(new CrcAckPacket() {
                            @Override
                            public void encode(ByteBuf to) {
                                throw new UnsupportedOperationException("client should not call this");
                            }
                        })
        );


        Bootstrap bootstrap = bootstrap(new Bootstrap())
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline() //
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
                .validate();
        this.connection_provider = () -> bootstrap.connect(address);
    }

    protected ChannelFuture newConnection() {
        return connection_provider.get();
    }

    public Promise<Channel> uploadFile(File file) {
        return Promise.costly(() -> uploadFile(file, 4 * 1024)).transformAsync((future) -> future);
    }

    protected Promise<Channel> uploadFile(File file, long chunk_size) throws Exception {
        UUID uuid = UUID.nameUUIDFromBytes((file.toURI().toString() + file.length()).getBytes());

        FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.READ);

        // how many chunks
        long chunks = file.length() / chunk_size
                + (file.length() % chunk_size != 0 ? 1 : 0);

        Channel socket = newConnection().channel();
        long max = file.length();
        return Promise.costly(() -> {
            LOGGER.info("start upload file:" + file);
            return LongStream.range(0, chunks).parallel()
                    .mapToObj((chunk_id) -> {
                        long absolute_offset = chunk_id * chunk_size;
                        long expected_size = chunk_size;
                        if (absolute_offset + expected_size > max) {
                            expected_size = max - absolute_offset;
                        }

                        try {
                            StreamChunkPacket chunk = new StreamChunkPacket(uuid, absolute_offset, channel.map(FileChannel.MapMode.READ_ONLY, absolute_offset, expected_size));
                            return Promise.wrap(socket.writeAndFlush(chunk));
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }).collect(Promise.collector());
        }).transform((ignore) -> {
            LOGGER.info("transfer:" + file.toURI() + " done, uuid:" + uuid);

            CRC32 crc = new CRC32();
            crc.update(channel.map(FileChannel.MapMode.READ_ONLY, 0, max));

            // relase file channel
            Promise.light(channel::close);

            LOGGER.info("file:" + file.toURI() + " uuid: " + uuid + " crc:" + crc.getValue());
            return socket.writeAndFlush(new CrcReportPacket(uuid, crc.getValue(), max));
        }).transform((ignore) -> socket)
                ;
    }
}

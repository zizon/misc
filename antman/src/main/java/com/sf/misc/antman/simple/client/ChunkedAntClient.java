package com.sf.misc.antman.simple.client;

import com.google.common.collect.Maps;
import com.sf.misc.antman.Promise;
import com.sf.misc.antman.simple.BootstrapAware;
import com.sf.misc.antman.simple.packets.Packet;
import com.sf.misc.antman.simple.PacketInBoundHandler;
import com.sf.misc.antman.simple.PacketOutboudHandler;
import com.sf.misc.antman.simple.packets.PacketRegistryAware;
import com.sf.misc.antman.simple.packets.RequestCRCAckPacket;
import com.sf.misc.antman.simple.packets.StreamChunkPacket;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

public class ChunkedAntClient implements PacketRegistryAware, BootstrapAware<Bootstrap> {

    public static final Log LOGGER = LogFactory.getLog(ChunkedAntClient.class);

    protected final ConcurrentMap<File, FileChannel> channels = new ConcurrentHashMap<>();
    protected final Promise.PromiseSupplier<Promise<Channel>> connection_provider;
    protected final Promise.PromiseSupplier<FileChunkAcks> acks;

    public ChunkedAntClient(SocketAddress address) {
        Packet.Registry registry =
                initializeRegistry(new Packet.Registry());

        Bootstrap bootstrap = bootstrap(new Bootstrap())
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline() //
                                .addLast(new PacketOutboudHandler())
                                .addLast(new PacketInBoundHandler(registry))
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
                .validate();

        connection_provider = newConnectionProvider(bootstrap, address);
        acks = newFileChunAcks();
    }

    public StreamFile newFile(UUID stream, File file) {
        long localize = file.length();

        return new StreamFile() {
            @Override
            public long size() {
                return localize;
            }

            @Override
            public long batch() {
                return 1024 * 1024 * 64;
            }

            @Override
            public UUID streamID() {
                return stream;
            }

            @Override
            public FileChunkAcks acks() {
                return ChunkedAntClient.this.acks.get();
            }

            @Override
            public ByteBuffer slice(long from, long to) {
                try {
                    return requestFileChannel(file).map(FileChannel.MapMode.READ_WRITE, from, to - from);
                } catch (IOException e) {
                    throw new UncheckedIOException("fail to map file:" + file + " range from:" + from + " to:" + to, e);
                }
            }

            @Override
            public Promise<?> send(StreamChunkPacket packet) {
                return ChunkedAntClient.this.send(packet);
            }
        };
    }

    @Override
    public Packet.Registry initializeRegistry(Packet.Registry registry) {
        return PacketRegistryAware.super.initializeRegistry(registry)
                .repalce(() -> new RequestCRCAckPacket() {
                    @Override
                    public void decodeComplete(ChannelHandlerContext ctx) {
                        acks.get().onReceiveAck(this);
                    }
                });
    }

    protected FileChannel requestFileChannel(File file) {
        return channels.computeIfAbsent(file, (key) -> {
            if (!file.exists()) {
                file.getParentFile().mkdirs();
            }

            try {
                return FileChannel.open(key.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
            } catch (IOException e) {
                throw new UncheckedIOException("fail to map file:" + key, e);
            }
        });
    }

    protected Promise<Channel> requestSocketChannel() {
        return connection_provider.get();
    }

    protected Promise<?> send(StreamChunkPacket packet) {
        return requestSocketChannel().transformAsync((channel) -> {
            return Promise.wrap(channel.writeAndFlush(packet));
        });
    }

    protected Promise.PromiseSupplier<FileChunkAcks> newFileChunAcks() {
        FileChunkAcks acks = new FileChunkAcks() {
            ConcurrentMap<Range, Promise<Long>> acks = Maps.newConcurrentMap();

            @Override
            public ConcurrentMap<Range, Promise<Long>> ackedRanges() {
                return acks;
            }
        };

        return () -> acks;
    }

    protected Promise.PromiseSupplier<Promise<Channel>> newConnectionProvider(Bootstrap bootstrap, SocketAddress address) {
        AtomicReference<Promise<Channel>> holder = new AtomicReference<>(null);
        return () -> {
            //TODO
            Promise<Channel> candiate = holder.get();
            if (candiate == null) {
                return newConnection(holder, bootstrap, address);
            }

            return candiate.transformAsync((channel) -> {
                if (channel.isActive()) {
                    return Promise.success(channel);
                }

                return newConnection(holder, bootstrap, address);
            });
        };
    }

    protected Promise<Channel> newConnection(AtomicReference<Promise<Channel>> holder, Bootstrap bootstrap, SocketAddress address) {
        //TODO
        return null;
    }
}

package com.sf.misc.antman.simple.client;

import com.sf.misc.antman.Promise;
import com.sf.misc.antman.simple.BootstrapAware;
import com.sf.misc.antman.simple.Packet;
import com.sf.misc.antman.simple.PacketInBoundHandler;
import com.sf.misc.antman.simple.PacketOutboudHandler;
import com.sf.misc.antman.simple.packets.CommitStreamAckPacket;
import com.sf.misc.antman.simple.packets.CommitStreamPacket;
import com.sf.misc.antman.simple.packets.PacketReigstryAware;
import com.sf.misc.antman.simple.packets.StreamChunkAckPacket;
import com.sf.misc.antman.simple.packets.StreamChunkPacket;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.AttributeKey;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.SocketAddress;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.zip.CRC32;

public class SimpleAntClient implements PacketReigstryAware, BootstrapAware<Bootstrap> {

    public static final Log LOGGER = LogFactory.getLog(SimpleAntClient.class);

    protected static final String COMMIT_ACK = "commit_ack";
    protected static final String SIZE_PROGRESS = "size_progress";

    protected final Promise.PromiseFunction<Promise.PromiseConsumer<Long>, Promise<Channel>> connection_provider;

    public SimpleAntClient(SocketAddress address) {
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

        this.connection_provider = (progress_callback) -> {
            ChannelFuture future = bootstrap.connect(address);

            // comit ack
            Promise<Boolean> commit_ack = Promise.promise();
            Channel channel = future.channel();
            channel.attr(AttributeKey.valueOf(COMMIT_ACK)).set(commit_ack);

            // size progress
            AtomicLong size = new AtomicLong();
            channel.attr(AttributeKey.valueOf(SIZE_PROGRESS)).set(size);

            // progress
            Promise<?> progress = Promise.period(() -> {
                progress_callback.accept(size.get());
            }, 5000);

            Promise.wrap(channel.closeFuture())
                    .catching((throwable) -> {
                        commit_ack.completeExceptionally(throwable);
                    })
                    .addListener(() -> {
                        progress.cancel(true);
                        LOGGER.info("cancle:" + progress);
                    });

            return Promise.wrap(future).transform((ignore) -> future.channel());
        };
    }

    protected Promise<Channel> newConnection(Promise.PromiseConsumer<Long> progress_callback) {
        return connection_provider.apply(progress_callback);
    }

    public Promise<Boolean> uploadFile(UUID stream_id, File file, Promise.PromiseConsumer<Long> progress_callback) {
        return Promise.costly(() -> uploadFile(stream_id, file, 4 * 1024, progress_callback)).transformAsync((through) -> through);
    }

    public Promise<Boolean> uploadFile(UUID uuid, File file, long chunk_size, Promise.PromiseConsumer<Long> progress_callback) throws Exception {
        long max = file.length();

        // size match notify
        Promise<?> size_match = Promise.promise();
        Promise.PromiseConsumer<Long> callback = (size) -> {
            if (size == max) {
                if (!size_match.isDone()) {
                    LOGGER.info("szie match");
                    size_match.complete(null);
                }
            }

            progress_callback.accept(size);
            return;
        };

        // create connection
        Promise<Channel> socket = newConnection(callback)
                .sidekick((channel) -> LOGGER.info("connection established:" + channel));

        // channel clsoe
        Promise<?> socket_closed = socket.transformAsync((channel) -> Promise.wrap(channel.closeFuture()))
                .catching((throwable) -> {
                    size_match.completeExceptionally(throwable);
                });

        // file mamp
        FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
        socket_closed.addListener(() -> channel.close());

        // calculate crc
        Promise<Long> crc = crc(channel, file)
                .sidekick((value) -> LOGGER.info("file:" + file + " crc:" + value));

        // send
        Promise<?> send = socket.transform((connection) -> {
            return generate(channel, uuid, max, chunk_size)
                    .map(connection::writeAndFlush)
                    .map(Promise::wrap)
                    .collect(Promise.collector())
                    ;
        }).sidekick(() -> LOGGER.info("write stream:" + uuid + " of file:" + file + " length:" + max));

        // commit
        Promise<?> stream_commit = size_match.transform((ignore) -> {
            return socket.join()
                    .writeAndFlush(new CommitStreamPacket(uuid, max, crc.join()));
        }).addListener(() -> {
            LOGGER.info("try commit stream:" + uuid + " file:" + file + " length:" + max + " crc:" + crc.join());
        });

        // close when commit
        return Promise.all(socket, stream_commit, socket_closed) //
                .transformAsync((ignore) -> {
                    Promise<Boolean> promise = socket.join().<Promise<Boolean>>attr(AttributeKey.valueOf(COMMIT_ACK)).get();
                    if (promise == null) {
                        throw new IllegalStateException("promise is null,stream:" + uuid + " file:" + file);
                    }
                    return promise;
                })
                .catching((throwable) -> {
                    Promise<Boolean> promise = socket.join().<Promise<Boolean>>attr(AttributeKey.valueOf(COMMIT_ACK)).get();
                    promise.completeExceptionally(throwable);
                });
    }

    protected Stream<StreamChunkPacket> generate(FileChannel channel, UUID uuid, long size, long chunk_size) {
        // how many chunks
        long chunks = size / chunk_size
                + (size % chunk_size != 0 ? 1 : 0);

        return LongStream.range(0, chunks)
                .mapToObj((chunk_id) -> {
                    long absolute_offset = chunk_id * chunk_size;
                    long expected_size = chunk_size;
                    if (absolute_offset + expected_size > size) {
                        expected_size = size - absolute_offset;
                    }

                    try {
                        StreamChunkPacket chunk = new StreamChunkPacket(uuid, absolute_offset, channel.map(FileChannel.MapMode.READ_ONLY, absolute_offset, expected_size));
                        return chunk;
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
    }

    protected Promise<Long> crc(FileChannel channel, File file) {
        return Promise.light(() -> {
            CRC32 crc = new CRC32();

            long limit = file.length();
            long offset = 0;
            while (offset < limit) {
                long lenght = Math.min(limit - offset, Integer.MAX_VALUE);
                crc.update(channel.map(FileChannel.MapMode.READ_ONLY, offset, lenght));
                offset += lenght;
            }

            return crc.getValue();
        });
    }

    @Override
    public Packet.Registry initializeRegistry(Packet.Registry registry) {
        return PacketReigstryAware.super.initializeRegistry(registry)
                .repalce(() -> new StreamChunkPacket(null, 0, null) {
                    @Override
                    public void decode(ByteBuf from) {
                        throw new UnsupportedOperationException("client should not call this");
                    }
                }) //
                .repalce(() -> new CommitStreamPacket() {
                    @Override
                    public void decodeComplete(ChannelHandlerContext ctx) {
                        throw new UnsupportedOperationException("client should not call this");
                    }
                })
                .repalce(() -> new CommitStreamAckPacket() {
                    @Override
                    public void encode(ByteBuf to) {
                        throw new UnsupportedOperationException("client should not call this");
                    }

                    @Override
                    public void decodeComplete(ChannelHandlerContext ctx) {
                        Promise<Boolean> promise = ctx.channel().<Promise<Boolean>>attr(AttributeKey.valueOf(COMMIT_ACK)).get();
                        promise.complete(this.match);

                        // clsoe it
                        LOGGER.info("ack:" + this + " closing");
                        ctx.close();
                    }
                }).repalce(() -> new StreamChunkAckPacket() {
                    @Override
                    public void encode(ByteBuf to) {
                        throw new UnsupportedOperationException("client should not call this");
                    }

                    @Override
                    public void decodeComplete(ChannelHandlerContext ctx) {
                        AtomicLong size = ctx.channel().<AtomicLong>attr(AttributeKey.valueOf(SIZE_PROGRESS)).get();
                        size.getAndAdd(this.length);
                    }
                });
    }
}

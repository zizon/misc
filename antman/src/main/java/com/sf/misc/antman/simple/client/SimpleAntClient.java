package com.sf.misc.antman.simple.client;

import com.sf.misc.antman.Promise;
import com.sf.misc.antman.simple.MemoryMapUnit;
import com.sf.misc.antman.simple.PacketInBoundHandler;
import com.sf.misc.antman.simple.PacketOutboudHandler;
import com.sf.misc.antman.simple.packets.CommitStreamAckPacket;
import com.sf.misc.antman.simple.packets.Packet;
import com.sf.misc.antman.simple.packets.PacketRegistryAware;
import com.sf.misc.antman.simple.packets.StreamChunkAckPacket;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.net.SocketAddress;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicLong;

public interface SimpleAntClient {

    public static final Log LOGGER = LogFactory.getLog(SimpleAntClient.class);

    static interface OutPacket {
        Packet packet();
    }

    static interface ChannelSession extends UploadSession {
        Promise<Channel> channel();

        default ChannelSession start() {
            UploadSession.super.start();
            return this;
        }
    }

    default MemoryMapUnit mmu() {
        return MemoryMapUnit.shared();
    }

    SocketAddress selectAddress(UUID stream_id);

    default UploadSession session(UUID stream_id) {
        return sessions().compute(stream_id, (key, old) -> {
            if (old == null) {
                throw new IllegalStateException("no session for stream:" + stream_id);
            }

            return old;
        });
    }

    static Promise<SimpleAntClient> create(SocketAddress address) {
        SimpleAntClient client = new SimpleAntClient() {
            ConcurrentMap<UUID, ChannelSession> sessions = new ConcurrentHashMap<>();

            @Override
            public SocketAddress selectAddress(UUID stream_id) {
                return address;
            }

            @Override
            public ConcurrentMap<UUID, ChannelSession> sessions() {
                return sessions;
            }
        };

        return Promise.success(client);
    }

    default Promise.PromiseSupplier<StreamChunkAckPacket> hookStreamChunkAck() {
        return () -> {
            return new StreamChunkAckPacket() {
                @Override
                public void decodeComplete(ChannelHandlerContext ctx) {
                    // noop
                    UploadSession session = session(stream_id);
                    session.commit(session.newRange(offset, length));
                }
            };
        };
    }

    default Promise.PromiseSupplier<CommitStreamAckPacket> hookCommitStreamAck() {
        return () -> {
            return new CommitStreamAckPacket() {
                public void decodeComplete(ChannelHandlerContext ctx) {
                    UploadSession session = session(stream_id);
                    if (match) {
                        session.completion().complete(null);
                    } else {
                        session.completion().completeExceptionally(
                                new IllegalStateException(
                                        "crc not match,stream:" + stream_id
                                                + " remote crc:" + crc
                                                + " local crc:" + session.crc()
                                )
                        );
                    }
                }
            };
        };
    }

    default Packet.Registry registry() {
        Packet.Registry registry = new PacketRegistryAware() {
        }.initializeRegistry(new Packet.Registry());

        // hook stream chunk ack
        registry.repalce(hookStreamChunkAck());
        registry.repalce(hookCommitStreamAck());

        return registry;
    }

    default Promise<Bootstrap> bootstrap() {
        Bootstrap bootstrap = new Bootstrap().group(new NioEventLoopGroup())
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<NioSocketChannel>() {
                    @Override
                    protected void initChannel(NioSocketChannel ch) throws Exception {
                        pipeline(ch.pipeline());
                    }
                })
                .validate();
        return Promise.success(bootstrap);
    }

    default ChannelPipeline pipeline(ChannelPipeline pipeline) {
        return pipeline.addLast(new PacketOutboudHandler())
                .addLast(new MessageToByteEncoder<OutPacket>() {
                    @Override
                    protected void encode(ChannelHandlerContext ctx, OutPacket msg, ByteBuf out) throws Exception {
                        msg.packet().encode(out);
                    }
                })
                .addLast(new PacketInBoundHandler(registry()))
                .addLast(new ChannelInboundHandlerAdapter() {
                    @Override
                    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
                            throws Exception {
                        // complete exceptional
                        sessions().entrySet().parallelStream()
                                .forEach((entry) -> {
                                    entry.getValue().channel().sidekick((candidate) -> {
                                        if (candidate.equals(ctx.channel())) {
                                            // same channel
                                            entry.getValue().completion().completeExceptionally(cause);
                                        }
                                    });
                                });

                        // send upstream
                        ctx.fireExceptionCaught(cause);
                    }
                })
                .addLast(new LoggingHandler(LogLevel.INFO));
    }

    default ChannelSession upload(UUID stream_id, File file) {
        return newSession(
                stream_id,
                file,
                (packet) -> writePacket(stream_id, packet)
        ).start();
    }

    ConcurrentMap<UUID, ChannelSession> sessions();

    default ChannelSession newSession(UUID uuid, File file, Promise.PromiseFunction<Packet, Promise<?>> io) {
        return sessions().compute(uuid, (key, old) -> {
            if (old != null) {
                throw new RuntimeException("duplicated session, old:" + old);
            }

            return createSession(uuid, file, io);
        });
    }

    default Promise<?> writePacket(UUID stream_id, Packet packet) {
        return sessions().get(stream_id).channel().transformAsync(
                (channel) -> Promise.wrap(channel.writeAndFlush(new OutPacket() {
                    @Override
                    public Packet packet() {
                        return packet;
                    }
                }))
        );
    }

    default Promise<Channel> createChannel(UUID stream_id) {
        return bootstrap().transformAsync((bootstrap) -> {
            ChannelFuture connecting = bootstrap.connect(selectAddress(stream_id));
            return Promise.wrap(connecting).transform((channel) -> {
                return connecting.channel();
            });
        });
    }

    default void cleanupSession(UUID stream_id) {
        Optional.ofNullable(sessions().remove(stream_id))
                .ifPresent((session) -> {
                    session.channel().sidekick(Channel::close);
                });
    }

    default ChannelSession createSession(UUID uuid, File file, Promise.PromiseFunction<Packet, Promise<?>> io) {
        return new ChannelSession() {
            NavigableMap<Range, Promise<?>> resolved = new ConcurrentSkipListMap<>();
            Promise<?> complection = Promise.promise();
            long crc = calculateCRC();
            Set<StateListener> listeners = new CopyOnWriteArraySet<>();
            Promise<Channel> channel = createChannel(uuid);

            {
                listeners.add(new StateListener() {
                    @Override
                    public void onProgress(long acked, long expected) {
                    }

                    @Override
                    public void onSuccess(long effected_time) {
                        cleanupSession(uuid);
                    }

                    @Override
                    public void onTimeout(Range range, long expire) {
                    }

                    @Override
                    public void onUnRecovable(Throwable reason) {
                        cleanupSession(uuid);
                    }

                    @Override
                    public boolean onFail(Range range, Throwable cause) {
                        return false;
                    }
                });
            }

            @Override
            public Promise<Channel> channel() {
                return channel;
            }

            @Override
            public UUID stream() {
                return uuid;
            }

            @Override
            public NavigableMap<Range, Promise<?>> resovled() {
                return resolved;
            }

            @Override
            public Promise<?> completion() {
                return complection;
            }

            @Override
            public long crc() {
                return crc;
            }

            @Override
            public File file() {
                return file;
            }

            @Override
            public Set<StateListener> listeners() {
                return listeners;
            }

            @Override
            public Promise.PromiseFunction<Packet, Promise<?>> io() {
                return io;
            }
        };
    }
}

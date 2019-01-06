package com.sf.misc.antman.simple.server;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.UUID;

public class StreamChunkPacket implements Packet {

    public static final Log LOGGER = LogFactory.getLog(StreamChunkPacket.class);

    public static final long PAGE_SIZE = 64 * 1024 * 1024; // 64m

    protected LoadingCache<UUID, FileChannel> channels = CacheBuilder.newBuilder()
            .expireAfterAccess(Duration.ofMinutes(1))
            .removalListener(new RemovalListener<UUID, FileChannel>() {
                @Override
                public void onRemoval(RemovalNotification<UUID, FileChannel> notification) {
                    try {
                        notification.getValue().close();
                    } catch (IOException e) {
                        LOGGER.warn("fail to close channel:" + notification.getValue() + " of file:" + notification.getKey() + e);
                    }
                }
            })
            .build(new CacheLoader<UUID, FileChannel>() {
                @Override
                public FileChannel load(UUID key) throws Exception {
                    return FileChannel.open(file(key).toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
                }
            });

    protected File storage;

    public StreamChunkPacket(File storage) {
        this.storage = storage;
        this.storage.mkdirs();
        if (!(storage.isDirectory() && storage.canWrite())) {
            throw new RuntimeException("storage:" + storage + " should be writable");
        }
    }

    @Override
    public void process(ChannelHandlerContext ctx, ByteBuf buf) throws Exception {
        long header = Long.BYTES + Long.BYTES  // uuid
                + Long.BYTES // offset
                + Long.BYTES // stream length
                ;

        UUID stream_id = new UUID(buf.readLong(), buf.readLong());
        long offset = buf.readLong();
        long chunk_length = buf.readLong();

        if (buf.readableBytes() != chunk_length) {
            throw new RuntimeException("lenght not match,expect:" + chunk_length + " got:" + buf.readableBytes());
        }

        ByteBuffer source = mmap(stream_id, offset, chunk_length);
        //LOGGER.info("handle a packet: sream id:" + stream_id + " offset:" + offset + " lenght:" + chunk_length + " source:" + source + " buf:" + buf);
        buf.readBytes(source);
    }

    protected FileChannel selectFile(UUID uuid) {
        return channels.getUnchecked(uuid);
    }

    protected File file(UUID uuid) {
        long bucket = uuid.getMostSignificantBits() % 100;
        File bucket_directory = new File(storage, "bucket-" + bucket);
        bucket_directory.mkdirs();

        File selected = new File(bucket_directory, uuid.toString());
        selected.getParentFile().mkdirs();
        return selected;
    }

    protected MappedByteBuffer mmap(UUID uuid, long offset, long length) throws IOException {
        FileChannel channel = selectFile(uuid);

        long max = offset + length;
        if (channel.size() > max) {
            channels.refresh(uuid);
            channel = selectFile(uuid);
        }

        return channel.map(FileChannel.MapMode.READ_WRITE, offset, length);
    }
}

package com.sf.misc.antman.simple.server;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.UUID;

public class ChunkServent {

    public static final Log LOGGER = LogFactory.getLog(ChunkServent.class);
    protected static File STORAGE;

    static {
        STORAGE = new File("__buckets__");
        STORAGE.mkdirs();
        if (!(STORAGE.isDirectory() && STORAGE.canWrite())) {
            throw new RuntimeException("storage:" + STORAGE + " should be writable");
        }
    }

    protected static LoadingCache<UUID, FileChannel> CHANNELS = CacheBuilder.newBuilder()
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

    public static FileChannel selectFile(UUID uuid) {
        return CHANNELS.getUnchecked(uuid);
    }

    public static File file(UUID uuid) {
        long bucket = uuid.getMostSignificantBits() % 100;
        File bucket_directory = new File(STORAGE, "bucket-" + bucket);

        File selected = new File(bucket_directory, uuid.toString());
        selected.getParentFile().mkdirs();
        return selected;
    }

    public static MappedByteBuffer mmap(UUID uuid, long offset, long length) throws IOException {
        FileChannel channel = selectFile(uuid);

        long max = offset + length;
        if (channel.size() > max) {
            CHANNELS.refresh(uuid);
            channel = selectFile(uuid);
        }

        return channel.map(FileChannel.MapMode.READ_WRITE, offset, length);
    }
}

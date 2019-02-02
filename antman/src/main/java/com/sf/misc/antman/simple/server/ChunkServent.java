package com.sf.misc.antman.simple.server;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.sf.misc.antman.Promise;
import com.sf.misc.antman.simple.MemoryMapUnit;
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

    public static File file(UUID uuid) {
        long bucket = uuid.getMostSignificantBits() % 100;
        File bucket_directory = new File(STORAGE, "bucket-" + bucket);

        File selected = new File(bucket_directory, uuid.toString());
        selected.getParentFile().mkdirs();
        return selected;
    }

    public static MemoryMapUnit mmu(){
        return MemoryMapUnit.shared();
    }

    public static Promise<ByteBuffer> mmap(UUID uuid, long offset, long length) {
        return mmu().map(file(uuid), offset, length);
    }

    public static Promise<?> unmap(ByteBuffer buffer) {
        return mmu().unmap(buffer);
    }

    public static void commit(UUID uuid, long length) throws IOException {
        //todo
    }
}

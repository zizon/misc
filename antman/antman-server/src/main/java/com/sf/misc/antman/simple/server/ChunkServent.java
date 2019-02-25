package com.sf.misc.antman.simple.server;

import com.sf.misc.antman.Promise;
import com.sf.misc.antman.simple.MemoryMapUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.UUID;

public class ChunkServent {

    public static final Log LOGGER = LogFactory.getLog(ChunkServent.class);
    public static File STORAGE;

    static {
        STORAGE = new File("__buckets__");
        STORAGE.mkdirs();
        if (!(STORAGE.isDirectory() && STORAGE.canWrite())) {
            throw new RuntimeException("storage:" + STORAGE + " should be writable");
        }
    }

    public static File file(UUID uuid) {
        long bucket = uuid.getMostSignificantBits() % 1000 + 1;
        File bucket_directory = new File(STORAGE, "bucket-" + bucket);

        long parition = uuid.getLeastSignificantBits() % 1000 + 1;
        File partition_directory = new File(bucket_directory, "partition-" + parition);

        File selected = new File(partition_directory, uuid.toString());
        selected.getParentFile().mkdirs();
        return selected;
    }

    public static MemoryMapUnit mmu() {
        return MemoryMapUnit.shared();
    }

    public static Promise<ByteBuffer> mmap(UUID uuid, long offset, long length) {
        return mmu().map(file(uuid), offset, length);
    }

    public static Promise<?> unmap(ByteBuffer buffer) {
        return mmu().unmap(buffer);
    }

    public static Promise<?> commit(UUID stream_id, UUID client_id) {
        //TODO
        return Promise.success(null);
    }
}

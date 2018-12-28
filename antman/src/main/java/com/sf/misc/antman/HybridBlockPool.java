package com.sf.misc.antman;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class HybridBlockPool {

    public static final Log LOGGER = LogFactory.getLog(HybridBlockPool.class);
    protected static final File STORAGE = new File("__hybrid_block_pool__");
    protected static final String RELEASED = "_released";
    protected static final String USING = "_using";
    protected static final String FINILIZED = "_finilized";
    protected static final Promise<Void> READY;

    static {
        // make dirs
        STORAGE.mkdirs();

        if (!STORAGE.isDirectory()) {
            LOGGER.error("could not make storge dir:" + STORAGE.toURI() + " quite");
            System.exit(-1);
        }

        // write test
        READY = ClosableAware.wrap(() -> new RandomAccessFile(new File(STORAGE, "__writeable__test__"), "rw"))
                .execute((file) -> {
                    LOGGER.info("wirte test ok for storage:" + STORAGE);
                })
                .catching((throwable) -> {
                    LOGGER.error("writable test fail for dirctory:" + STORAGE, throwable);
                })
                .addListener(() -> cleanup(true))
                .transform((ignore) -> {
                    // all ok,start scheudler
                    Promise.period( //
                            () -> {
                                LOGGER.info("trigger cleanup...");
                                cleanup(true);
                            }, //
                            TimeUnit.MINUTES.toMillis(5), (throwable) -> {
                                LOGGER.warn("clean up fail with exception", throwable);
                            } //
                    );
                    return null;
                });
    }

    public static Map.Entry<UUID, MappedByteBuffer> allocate(long size) {
        // block untile ready
        READY.maybe();

        UUID uuid = UUID.randomUUID();
        File backed = new File(STORAGE, uuid.toString() + USING);

        try (RandomAccessFile ignore = new RandomAccessFile(backed, "rw")) {
            ignore.setLength(size);
        } catch (IOException e) {
            throw new UncheckedIOException("fail to create backed memroy of size:" + size + " candiate file:" + backed, e);
        }

        try (FileChannel channel = FileChannel.open(backed.toPath(), StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE)) {
            return new AbstractMap.SimpleEntry<>(uuid, channel.map(FileChannel.MapMode.READ_WRITE, 0, size));
        } catch (IOException e) {
            throw new UncheckedIOException("file to mmap file:" + backed + " of size:" + size, e);
        }
    }

    public static void release(UUID buffer_id) {
        String uuid = buffer_id.toString();
        Arrays.stream(STORAGE.listFiles()).parallel()
                .filter((file) -> file.getName().startsWith(buffer_id.toString()))
                .filter((file) -> !file.getName().contains(RELEASED))
                .forEach((file) -> {
                    file.renameTo(new File(file.getParent(), file.getName() + RELEASED));
                });
        return;
    }

    public static void finalized(UUID buffer_id) {
        String uuid = buffer_id.toString();
        Arrays.stream(STORAGE.listFiles()).parallel()
                .filter((file) -> file.getName().startsWith(buffer_id.toString()))
                .filter((file) -> file.getName().contains(FINILIZED))
                .forEach((file) -> {
                    file.renameTo(new File(file.getParent(), file.getName() + FINILIZED));
                });
        return;
    }

    protected static boolean expire(long time) {
        return System.currentTimeMillis() - time > TimeUnit.DAYS.toMillis(1);
    }

    protected static void cleanup(boolean include_expire) {
        Arrays.stream(STORAGE.listFiles()).parallel()
                .filter((file) -> {
                    if (include_expire) {
                        // means all
                        return true;
                    }

                    // hide files that not expires
                    return !expire(file.lastModified());
                }) // exclude finiized
                .filter((file) -> !file.getName().contains(FINILIZED))
                .forEach((file) -> {
                    Promise.submit(file::delete)
                            .sidekick(() -> LOGGER.info("delete file:" + file.toURI()))
                            .catching((throwable) -> {
                                LOGGER.error("fail to delete file:" + file.toURI(), throwable);
                            });
                });
        ;
    }

}

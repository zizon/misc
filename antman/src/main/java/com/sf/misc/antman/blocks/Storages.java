package com.sf.misc.antman.blocks;

import com.sf.misc.antman.Promise;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class Storages {

    public static final Log LOGGER = LogFactory.getLog(Storages.class);

    protected static final String PREFIX = "block-";
    protected static final File STORAGE = ensureDirectory(new File("__block_storage__"));
    protected static final File RECLAIM = ensureDirectory(new File(STORAGE, "reclaim"));
    protected static final File LEASED = ensureDirectory(new File(STORAGE, "leased"));
    protected static final File EXPIRED = ensureDirectory(new File(STORAGE, "expired"));
    protected static final File TRASH = ensureDirectory(new File(STORAGE, "trash"));

    static {
        // some clean up

        // exipre files
        Promise.period(Storages::expireBlockChannels, TimeUnit.MINUTES.toMillis(1));
        Promise.period(Storages::shrikReclaim, TimeUnit.MINUTES.toMillis(1));
        Promise.period(Storages::emptyTrash, TimeUnit.MINUTES.toMillis(1));
    }

    protected static void expireBlockChannels() {
        Arrays.stream(LEASED.listFiles()).parallel()
                .filter((file) -> file.getName().startsWith(PREFIX))
                .filter(Storages::exipred)
                .forEach((file) -> {
                    // move to reclaim
                    move(file, new File(RECLAIM, file.getName()));
                });
    }

    protected static void shrikReclaim() {
        Arrays.stream(RECLAIM.listFiles()).parallel()
                .filter((file) -> file.getName().startsWith(PREFIX))
                .skip(20) // keep 20 files
                .forEach((file) -> {
                    move(file, new File(TRASH, file.getName()));
                });
    }

    protected static void emptyTrash() {
        Arrays.stream(TRASH.listFiles()).parallel()
                .filter((file) -> file.getName().startsWith(PREFIX))
                .forEach((file) -> {
                    // move to reclaim
                    file.delete();
                });
    }

    protected static boolean exipred(File file) {
        //TODO,tune times
        return file.lastModified() - System.currentTimeMillis() > TimeUnit.MINUTES.toMillis(5);
    }

    protected static File ensureDirectory(File file) {
        if (!file.exists()) {
            STORAGE.mkdirs();
        }

        if (!file.isDirectory()) {
            throw new RuntimeException("should be direcotry:" + STORAGE);
        }

        return file;
    }

    protected static void move(File from, File to) {
        // move to reclaim
        try {
            Files.move(from.toPath(), to.toPath(), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.COPY_ATTRIBUTES);
        } catch (IOException e) {
            LOGGER.warn("fail to move file:" + from + " to target:" + to, e);
        }
    }

    public static class BlockChannel {
        public FileChannel channel() {
            return null;
        }

        public File file() {
            return null;
        }
    }

    public static BlockChannel lease() {

        return null;
    }

    public static void reclaim(BlockChannel channel) {
        File file = channel.file();
        move(file, new File(RECLAIM, file.getName()));
    }
}

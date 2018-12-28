package com.sf.misc.antman;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

public class MMap {

    public static final Log LOGGER = LogFactory.getLog(MMap.class);


    public static final byte VERSION = 0x01;

    protected static final long VERSION_SIZE = 1;
    protected static final long PAGE_SIZE = 64 * 1024 * 1024;//64m
    protected static final int PAGES_PER_FILE = 128;
    protected static final long LOGICAL_FILE_SIZE = PAGE_SIZE * PAGES_PER_FILE;
    protected static final int MASK_ALIGN = Long.BYTES;
    protected static final long ALLOCATION_MAP_BYTES = PAGES_PER_FILE / MASK_ALIGN * 8;
    protected static final long ALLOCATION_OFFSET = VERSION_SIZE;
    protected static final long HEADER_SIZE = ALLOCATION_MAP_BYTES + VERSION_SIZE;
    protected static final long FILE_SIZE = LOGICAL_FILE_SIZE + HEADER_SIZE;

    protected static final String PREFIX = "block-";

    static {
        // size check
        if (ALLOCATION_MAP_BYTES % (MASK_ALIGN) != 0) {
            throw new IllegalArgumentException("page per file should be multiple of 8,configurated:" + PAGES_PER_FILE);
        }
    }

    public static class Page {

        protected List<Map.Entry<MMapFile, MappedByteBuffer>> buffers;

        public Page(List<Map.Entry<MMapFile, MappedByteBuffer>> buffers, long limit) {
            this.buffers = buffers;
        }
    }

    protected static class MMapFile {

        protected final FileLock lock;
        protected MappedByteBuffer underlying_mask;

        public MMapFile(FileLock lock, boolean format) {
            this.lock = lock;

            if (format) {
                this.format();
            }
        }

        protected MappedByteBuffer reloadMask() {
            if (this.underlying_mask == null) {
                operateInChannel((channel) -> {
                    MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, ALLOCATION_OFFSET, ALLOCATION_MAP_BYTES);
                    synchronized (this) {
                        if (this.underlying_mask == null) {
                            this.underlying_mask = buffer;
                        }
                    }
                });
            }

            return this.underlying_mask;
        }

        protected void operateInChannel(Promise.PromiseConsumer<FileChannel> consumer) {
            FileChannel channel = this.lock.channel();
            try {
                consumer.accept(channel);
            } catch (Throwable e) {
                try {
                    lock.close();
                } catch (IOException exception) {
                    LOGGER.error("release lock fail", exception);
                } finally {
                    throw new RuntimeException("fail to format channel:" + channel, e);
                }
            }
        }

        protected void format() {
            this.operateInChannel((channel) -> {
                MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, HEADER_SIZE);
                buffer.put(VERSION);

                // seek to mask
                buffer.position((int) ALLOCATION_OFFSET);
                // fast put

                // clear mask
                LongStream.range(0, ALLOCATION_MAP_BYTES)
                        .forEach((ignore) -> buffer.put((byte) 0));
            });
        }

        public MappedByteBuffer allocate() {
            ByteBuffer mask = reloadMask().duplicate();

            int selcetd = -1;

            // block others
            synchronized (this) {
                for (int i = 0; i < ALLOCATION_MAP_BYTES; i++) {
                    ByteBuffer update = mask.duplicate();
                    byte bucket = (byte) (mask.get() & 0b11111111);
                    // bit lookup
                    if ((bucket & 0b00000001) == 0) {
                        // not used
                        selcetd = i * 8 + 8;
                        update.put((byte) (bucket | 0b00000001));
                        break;
                    } else if ((bucket & 0b00000010) == 0) {
                        // not used
                        selcetd = i * 8 + 7;
                        update.put((byte) (bucket | 0b00000010));
                        break;
                    } else if ((bucket & 0b00000100) == 0) {
                        // not used
                        selcetd = i * 8 + 6;
                        update.put((byte) (bucket | 0b00000100));
                        break;
                    } else if ((bucket & 0b00001000) == 0) {
                        // not used
                        selcetd = i * 8 + 5;
                        update.put((byte) (bucket | 0b00001000));
                        break;
                    } else if ((bucket & 0b00010000) == 0) {
                        // not used
                        selcetd = i * 8 + 4;
                        update.put((byte) (bucket | 0b00010000));
                        break;
                    } else if ((bucket & 0b00100000) == 0) {
                        // not used
                        selcetd = i * 8 + 3;
                        update.put((byte) (bucket | 0b00100000));
                        break;
                    } else if ((bucket & 0b01000000) == 0) {
                        // not used
                        selcetd = i * 8 + 2;
                        update.put((byte) (bucket | 0b01000000));
                        break;
                    } else if ((bucket & 0b10000000) == 0) {
                        // not used
                        selcetd = i * 8 + 1;
                        update.put((byte) (bucket | 0b10000000));
                        break;
                    } else {
                        throw new RuntimeException("should not here");
                    }
                }
            }

            if (selcetd > 0) {
                try {
                    return lock.channel().map(FileChannel.MapMode.READ_WRITE, HEADER_SIZE + selcetd * PAGE_SIZE, PAGE_SIZE);
                } catch (IOException e) {
                    LOGGER.warn("fail to allocate buffer");
                    return null;
                }
            }
            return null;
        }

        public int pollBlock() {
            return -1;
        }
    }

    protected final File storage;
    protected final Set<MMapFile> mmap_files;

    public MMap(Path storage) {
        this.storage = storage.toFile();
        this.mmap_files = Sets.newConcurrentHashSet();

        this.initializeStorage(this.storage, this.mmap_files);
    }

    protected void initializeStorage(File storage, Set<MMapFile> mmap_files) {
        // exists?
        if (!storage.exists()) {
            storage.mkdirs();
        }

        if (!storage.isDirectory()) {
            throw new RuntimeException("storage is not a directory:" + storage);
        }

        // recover old files
        Arrays.stream(storage.listFiles()).parallel()
                .filter((file) -> file.getName().startsWith(PREFIX))
                .forEach((file) -> this.addFile(file, mmap_files));
    }

    protected void addFile(File file, Set<MMapFile> mmap_files) {
        try (RandomAccessFile opend = new RandomAccessFile(file, "rw")) {
            LOGGER.info("add mmap file:" + file);
            mmap_files.add(new MMapFile(opend.getChannel().lock(), false));
            return;
        } catch (IOException e) {
            LOGGER.error("fail to add mmap file" + file, e);
            return;
        }
    }
}

package com.sf.misc.antman.io;

import com.sf.misc.antman.ClosableAware;
import com.sf.misc.antman.Promise;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.LongStream;

public class FileStore {

    public static final Log LOGGER = LogFactory.getLog(FileStore.class);


    public static class Block {
        protected static final ConcurrentMap<Block, ByteBuffer> LEASER = new ConcurrentHashMap<>();

        static {
            Promise.period(() -> {
                LEASER.clear();
            }, TimeUnit.SECONDS.toMillis(1));
        }

        protected final long page_id;
        protected final long block_id;
        protected final boolean is_used;
        protected final MMU mmu;
        private SoftReference<ByteBuffer> buffer;


        protected Block(MMU mmu, long page_id, long block_id, boolean is_used) {
            this(mmu, page_id, block_id, is_used, null);
        }

        private Block(MMU mmu, long page_id, long block_id, boolean is_used, ByteBuffer buffer) {
            this.mmu = mmu;
            this.page_id = page_id;
            this.block_id = block_id;
            this.is_used = is_used;
            this.buffer = new SoftReference<>(buffer);
        }

        public boolean isUsed() {
            return is_used;
        }

        public Block use(boolean in_used) {
            return new Block(mmu, page_id, block_id, in_used, this.buffer.get());
        }

        public static long capacity() {
            return BLOCK_SIZE - 1;
        }

        public Promise<Void> write(Promise.PromiseFunction<ByteBuffer, Promise<Void>> consumer) {
            return mayLoad().transform((write) -> {
                // mark in used flag
                byte inused = write.get();
                if (inused != 0x01) {
                    throw new IllegalStateException("block state not in inused:" + inused);
                }

                // strip the in used byte
                return consumer.apply(write.slice());
            }).transformAsync((through) -> through);
        }

        public Promise<ByteBuffer> zone() {
            return this.mayLoad().transform((buffer) -> {
                // skip in used
                byte inused = buffer.get();
                if (inused != 0x01) {
                    throw new IllegalStateException("block state not in inused:" + inused);
                }

                return buffer.slice();
            });
        }

        public Promise<Void> free() {
            return mmu.release(this);
        }

        protected Promise<ByteBuffer> mayLoad() {
            // try if not gced
            ByteBuffer local = buffer.get();
            if (local != null) {
                LEASER.put(this, local);
                return Promise.success(local.duplicate());
            }

            // then try load
            return mmu.channel().transformAsync((mmu_channel) -> {
                return ClosableAware.wrap(() -> mmu_channel).transform((channel) -> {
                    long page_offset = page_id * PAGE_SIZE;
                    long block_offset = page_offset + block_id * BLOCK_SIZE;

                    ByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, block_offset, BLOCK_SIZE);
                    // update reference
                    // it is ok that difference thread reference differten buffer instance,
                    // as the underlying memory *SHOULD* be the same.
                    this.buffer = new SoftReference<>(buffer);

                    LEASER.put(this, buffer);
                    return buffer.duplicate();
                });
            });
        }

        @Override
        public String toString() {
            return "block[page_id:" + this.page_id + " block_id:" + this.block_id + "";
        }

    }

    public static class MMU {

        protected final File storage;
        protected final NavigableSet<Block> block_pool;
        protected final NavigableSet<Block> dangle;
        protected final Promise.PromiseSupplier<Promise<Void>> block_allocator;

        protected MMU(File storage, NavigableSet<Block> block_pool, NavigableSet<Block> dangle, Promise.PromiseSupplier<Promise<Void>> block_allocator) {
            this.storage = storage;
            this.block_pool = block_pool;
            this.dangle = dangle;
            this.block_allocator = block_allocator;
        }

        public Promise<Block> request() {
            // try from block pool
            Block allocated = this.block_pool.pollFirst();
            if (allocated != null) {
                return allocated.mayLoad().transform((buffer) -> {
                    ByteBuffer local = buffer.duplicate();

                    // BUG ON;
                    // check in used state
                    if (local.get() == 0x01 || allocated.isUsed()) {
                        throw new RuntimeException("block:" + allocated + " is in used,should be free");
                    }

                    ByteBuffer write = buffer.duplicate();
                    write.put((byte) 0x01);

                    // adjust in used state
                    return allocated.use(true);
                });
            }

            // rqeust allocation,then try again
            //LOGGER.info("try expand:" + this.block_pool.size() + " dangle:" + dangle.size());
            LOGGER.info("try expand:");
            return this.block_allocator.get().transformAsync((ignore) -> {
                LOGGER.info("another requset afater expand");
                return request();
            });
        }

        public Promise<Void> release(Block block) {
            if (block == null) {
                return Promise.success(null);
            }

            return block.mayLoad().transform((write) -> {
                // update in used byte
                write.put((byte) 0x0);

                // sync in used state
                block_pool.add(block.use(false));
                return null;
            });
        }

        public int esitamteFree() {
            return this.block_pool.size();
        }

        public int estiamteUsed() {
            return this.dangle.size();
        }

        public NavigableSet<Block> dangle() {
            return this.dangle;
        }

        protected Promise<FileChannel> channel() {
            return Promise.light(() -> FileChannel.open(storage.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE));
        }
    }

    protected static final long PAGE_SIZE = 64 * 1024 * 1024;
    protected static final long BLOCK_SIZE = 4 * 1024;

    protected final Promise<MMU> mmu;
    protected final NavigableSet<Block> block_pools;
    protected final NavigableSet<Block> dangle;
    protected final AtomicReference<Promise<Void>> expanding;

    public FileStore(File storage) {
        this.block_pools = new ConcurrentSkipListSet<>(newBlockCompartor((in_used) -> !in_used));
        this.dangle = new ConcurrentSkipListSet<>(newBlockCompartor((in_used) -> in_used));
        this.expanding = new AtomicReference<>(null);

        this.mmu = ensureFile(storage).sidekick(() -> LOGGER.info("initialize storage:" + storage))
                .transformAsync(this::minimumSize).sidekick((valid) -> LOGGER.info("size ok:" + valid + " size:" + valid.length()))
                .transformAsync((valid_storage) -> {
                    return recoverIfAny(new MMU(storage, this.block_pools, this.dangle, this::expand), valid_storage, block_pools, dangle).sidekick(() -> LOGGER.info("recover storage:" + storage));
                });
    }

    public Promise<MMU> mmu() {
        return this.mmu;
    }

    protected Comparator<Block> newBlockCompartor(Function<Boolean, Boolean> accept_in_used_state) {
        return (left, right) -> {
            if (!accept_in_used_state.apply(left.isUsed())) {
                throw new IllegalStateException("block reject for mismatch in used state:" + left);
            } else if (!accept_in_used_state.apply(right.isUsed())) {
                throw new IllegalStateException("block reject for mismatch in used state:" + left);
            }

            int page_equality = Long.compare(left.page_id, right.page_id);
            if (page_equality != 0) {
                return page_equality;
            }

            return Long.compare(left.block_id, right.block_id);
        };
    }

    protected Promise<Void> expand() {
        Promise<Void> promise = Promise.promise();
        if (!this.expanding.compareAndSet(null, promise)) {
            // some other is expanding
            Promise<Void> current_expander = this.expanding.get();
            if (current_expander != null) {
                return current_expander;
            }

            // or a expand just finished
            return Promise.success(null);
        }

        // now as expander
        this.mmu.transformAsync((mmu) -> {
            File file = mmu.storage;
            long current = file.length();
            long current_pages = current / PAGE_SIZE;
            long new_pages = 10;
            long expected = current + new_pages * PAGE_SIZE;
            long blocks_per_page = PAGE_SIZE / BLOCK_SIZE;

            return ClosableAware.wrap(() -> new RandomAccessFile(file, "rw")).transform((storage) -> {
                storage.setLength(expected);

                LongStream.range(0, current_pages + new_pages).parallel()
                        .forEach((page_id) -> {
                            LongStream.range(0, blocks_per_page).parallel()
                                    .forEach((block_id) -> {
                                        block_pools.add(new Block(mmu, page_id, block_id, false));
                                    });
                        });
                return null;
            });
        }).sidekick((ignore) -> {
            // release
            if (!this.expanding.compareAndSet(promise, null)) {
                promise.completeExceptionally(new RuntimeException("expander not match"));
            } else {
                promise.complete(null);
            }
        }).catching((throwable) -> {
            if (!this.expanding.compareAndSet(promise, null)) {
                promise.completeExceptionally(new RuntimeException("expander not match", throwable));
            } else {
                promise.completeExceptionally(throwable);
            }
        });
        return promise;
    }

    protected Promise<File> ensureFile(File target) {
        File local = Optional.of(target).get();

        if (!local.exists()) {
            return ClosableAware.wrap(() -> new RandomAccessFile(local, "rw")).transform((file) -> {
                return local;
            });
        } else if (!local.isFile()) {
            return Promise.exceptional(() -> new RuntimeException("file:" + local + " is not a file"));
        } else if (!(local.canRead() && local.canWrite())) {
            return Promise.exceptional(() -> new IOException("file:" + local + " should be read/writeable"));
        }

        return Promise.success(target);
    }

    protected Promise<File> minimumSize(File file) {
        return minimumSize(file, file.length());
    }

    protected Promise<File> minimumSize(File file, long hint) {
        // at least 10 pages
        if (hint <= 0) {
            hint = PAGE_SIZE * 10;
        }

        // is large enough
        long file_size = file.length();
        if (file_size > hint) {
            return Promise.success(file);
        }

        // do rouding
        long rounded = hint - hint % PAGE_SIZE;
        return ClosableAware.wrap(() -> new RandomAccessFile(file, "rw"))
                .transform((opend) -> {
                    opend.setLength(rounded);
                    return file;
                });
    }

    protected Promise<MMU> recoverIfAny(MMU mmu, File storage, Set<Block> block_pools, Set<Block> used_blocks) {
        long pages = storage.length() / PAGE_SIZE;
        long blocks = PAGE_SIZE / BLOCK_SIZE;

        return Promise.light(() -> FileChannel.open(storage.toPath(), StandardOpenOption.READ))
                .transformAsync((channel) -> {
                    return LongStream.range(0, pages).parallel()
                            .mapToObj((page_id) -> {
                                return LongStream.range(0, blocks).parallel()
                                        .mapToObj((block_id) -> {
                                            return Promise.light(() -> {
                                                long page_offset = page_id * PAGE_SIZE;
                                                long block_offset = page_offset + block_id * BLOCK_SIZE;

                                                // use promise to use limit concurrency
                                                ByteBuffer block_raw = channel.map(FileChannel.MapMode.READ_ONLY, block_offset, BLOCK_SIZE);
                                                byte state = block_raw.get();
                                                switch (state) {
                                                    case 0x00:
                                                        block_pools.add(new Block(mmu, page_id, block_id, false));
                                                        break;
                                                    case 0x01:
                                                        used_blocks.add(new Block(mmu, page_id, block_id, true));
                                                        break;
                                                    default:
                                                        throw new IllegalStateException("page state not illage:" + state);
                                                }
                                            });
                                        });
                            })
                            .flatMap((stream) -> stream)
                            .collect(Promise.collector())
                            .addListener(() -> channel.close());
                }).transform((ignore) -> mmu);
    }

}

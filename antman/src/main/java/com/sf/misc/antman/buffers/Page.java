package com.sf.misc.antman.buffers;

import com.google.common.base.Strings;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.sf.misc.antman.Promise;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public abstract class Page {

    public static final long SIZE = 64 * 1024 * 1024;// 64M

    public static interface PageType extends Comparable<PageType> {
        public byte type();

        default public boolean compatible(PageType type) {
            return type != null && this.type() == type.type();
        }

        default public int compareTo(PageType other) {
            return other == null ? 1 : type() - other.type();
        }

        default public String string() {
            return "PageType(+" + this.getClass() + "):" + type();
        }
    }

    public static interface PageProcessor extends PageType {

        public Promise<Void> period();

        public Promise<PageProcessor> bind(InfiniteStore store);

        public Promise<Page> allocate();
    }

    public static abstract class AbstractPageProcessor implements PageProcessor {

        public static final Log LOGGER = LogFactory.getLog(AbstractPageProcessor.class);

        protected static ConcurrentMap<ResizableFileChannel, ConcurrentSkipListMap<Long, Boolean>> LOCKS = Maps.newConcurrentMap();

        protected InfiniteStore store;
        protected ConcurrentMap<Long, Boolean> allocation_map = Maps.newConcurrentMap();

        abstract protected Promise<Void> touchPage(long page_id);

        @Override
        public Promise<PageProcessor> bind(InfiniteStore store) {
            return Promise.submit(() -> {
                this.store = Optional.of(store).get();
                return this;
            });
        }

        @Override
        public String toString() {
            return string();
        }

        @Override
        public Promise<Page> allocate() {
            return store().transformAsync((store) -> {
                return store.channel.transformAsync((channel) -> {
                    ConcurrentSkipListMap<Long, PageProcessor> page_owners = store.owners;
                    for (; ; ) {
                        // try accquire
                        long limit = channel.size();
                        if (limit % SIZE != 0) {
                            // check size
                            throw new IllegalStateException("file size should be multiple of " + SIZE);
                        }
                        long last_not_used_page_id = limit / SIZE;

                        for (long page_id = 0; page_id < last_not_used_page_id; page_id++) {
                            boolean accquird = page_owners.putIfAbsent(page_id, this) == null;
                            if (!accquird) {
                                continue;
                            }

                            // accquird
                            Optional<Promise<Page>> page = this.allocate(page_id);
                            if (page.isPresent()) {
                                // allocation fail
                                // allocated
                                return page.get();
                            }

                            // release
                            page_owners.remove(page_id, this);
                        }

                        LOGGER.warn("allocate no page, try exnpand and retry...");

                        // find no free page,try expand
                        int batch = batch();
                        long length = limit + batch * SIZE;
                        LOGGER.info("expand file to:" + length);
                        channel.setLength(length);
                    }
                });
            });
        }

        @Override
        public Promise<Void> period() {
            return store().transform((store) -> store.owners)
                    .transform((owners) -> {
                        return owners.entrySet().parallelStream().filter((entry) -> this.compatible(entry.getValue()))
                                .map(Map.Entry::getKey)
                                .map(this::touchPage)
                                .toArray(Promise[]::new);
                    })
                    .transformAsync(Promise::all);
        }

        protected Promise<InfiniteStore> store() {
            return Promise.submit(() -> Optional.ofNullable(this.store))
                    .transform((optional) -> optional.get());
        }

        protected Promise<ResizableFileChannel> channel() {
            return this.store().transformAsync((store) -> store.channel);
        }

        protected Optional<Promise<Page>> allocate(long page_id) {
            // put should return null or false,
            boolean accquired = allocation_map.putIfAbsent(page_id, true) == null;
            if (!accquired) {
                return Optional.empty();
            }

            return Optional.ofNullable(this.mmap(page_id, FileChannel.MapMode.READ_WRITE)
                    .<Page>transform(
                            (buffer) -> {
                                ByteBuffer newly = buffer.duplicate();
                                ByteBuffer meta = newly.duplicate();

                                // meta
                                meta.put((byte) 0b01); // in used
                                meta.put(this.type()); // page type
                                return new Page(buffer) {
                                    @Override
                                    public void reclaim() {
                                        allocation_map.remove(page_id, true);
                                        newly.duplicate().put((byte) 0); // free it
                                    }
                                };
                            }
                    ) //
                    .catching((throwable) -> {
                        // release if mmap fail
                        allocation_map.remove(page_id, true);
                    })
            );
        }

        protected Promise<MappedByteBuffer> mmap(long block_id, FileChannel.MapMode mode) {
            return this.channel().transform((channel) -> {
                return channel.map(mode, block_id * SIZE, SIZE);
            });
        }

        protected int batch() {
            return 10;
        }
    }

    protected final MappedByteBuffer buffer;

    protected Page(MappedByteBuffer buffer) {
        this.buffer = buffer;
    }

    public void sync() {
        this.buffer.force();
    }

    protected MappedByteBuffer underlying() {
        return this.buffer;
    }

    abstract public void reclaim();
}

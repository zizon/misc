package com.sf.misc.antman.buffers;

import com.google.common.base.Strings;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.sf.misc.antman.Promise;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.lang.ref.SoftReference;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

public abstract class Page {

    public static final long SIZE = 64 * 1024 * 1024;// 64M

    public static interface PageType {
        public byte type();

        default public boolean compatible(PageType type) {
            return type != null && this.type() == type.type();
        }

        default public String string() {
            return "PageType(+" + this.getClass() + "):" + type();
        }
    }

    public static interface PageProcessor extends PageType {

        public Promise<Void> period();

        public PageProcessor bind(ResizableFileChannel channel, ConcurrentSkipListMap<Long, PageProcessor> owners);


        public Promise<Page> allocate();
    }

    public static abstract class AbstractPageProcessor implements PageProcessor {

        public static final Log LOGGER = LogFactory.getLog(AbstractPageProcessor.class);

        protected static ConcurrentMap<ResizableFileChannel, ConcurrentSkipListMap<Long, Boolean>> LOCKS = Maps.newConcurrentMap();

        protected ResizableFileChannel channel;
        protected ConcurrentSkipListMap<Long, Page.PageProcessor> owners;

        abstract protected Page initializeBuffer(MappedByteBuffer buffer);

        abstract protected void touchPage(List<Long> offest);

        @Override
        public PageProcessor bind(ResizableFileChannel channel, ConcurrentSkipListMap<Long, PageProcessor> owners) {
            this.channel = Optional.of(channel).get();
            this.owners = Optional.of(owners).get();
            return this;
        }

        @Override
        public String toString() {
            return string();
        }

        @Override
        public Promise<Page> allocate() {
            return Promise.submit(() -> {
                // reference channel
                ResizableFileChannel channel = Optional.of(this.channel).get();
                ConcurrentSkipListMap<Long, PageProcessor> owners = Optional.of(this.owners).get();

                // try accquire
                long limit = channel.size();
                if (limit % SIZE != 0) {
                    // check size
                    throw new IllegalStateException("file size should be multiple of " + SIZE);
                }

                // find usable
                for (Map.Entry<Long, PageProcessor> entry : owners.entrySet()) {
                    PageProcessor holder = owners.compute(entry.getKey(), (block_id, value) -> {
                        if (value == null) {
                            // accquire it
                            return this;
                        }
                        return value;
                    });

                    // accquird
                    if (holder == this) {
                        //  make page
                        MappedByteBuffer buffer = this.channel.map(
                                FileChannel.MapMode.READ_WRITE,
                                entry.getKey(),
                                SIZE
                        );

                        buffer.put((byte) 0b01); // use mark
                        buffer.put(type());  // page type

                        // allocated
                        return Optional.of(initializeBuffer(buffer));
                    }
                }

                // find no free page,try expand
                channel.setLength(channel.size() + batch() * SIZE);
                return Optional.<Page>empty();
            }).costly().transform((optional) -> {
                if (optional.isPresent()) {
                    return optional.get();
                }

                // try again
                return allocate().maybe().orElseThrow(() -> new RuntimeException("second try allocation fail for processor:" + this));
            });

        }

        @Override
        public Promise<Void> period() {
            return Promise.submit(() -> {
                List<Long> pages = owners.entrySet().parallelStream().filter((entry) -> this.compatible(entry.getValue()))
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toList());

                this.touchPage(pages);
                return null;
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

    abstract void reclaim();

}

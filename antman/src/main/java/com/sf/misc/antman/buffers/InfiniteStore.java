package com.sf.misc.antman.buffers;

import com.google.common.collect.Maps;
import com.sf.misc.antman.ClosableAware;
import com.sf.misc.antman.Promise;
import com.sf.misc.antman.buffers.processors.MutablePage;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.StandardOpenOption;
import java.security.Guard;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class InfiniteStore {

    public static final Log LOGGER = LogFactory.getLog(InfiniteStore.class);

    protected final Set<Page.PageProcessor> processors;
    protected final Promise<ResizableFileChannel> channel;
    protected final ConcurrentSkipListMap<Long, Page.PageProcessor> owners;
    protected final Promise<InfiniteStore> ready;

    public InfiniteStore(File storage) {
        this.processors = new ConcurrentSkipListSet<>();
        this.owners = new ConcurrentSkipListMap<>();
        this.channel = Promise.submit(() -> new ResizableFileChannel(storage));
        this.ready = recover();
    }

    public Promise<InfiniteStore> ready() {
        return this.ready;
    }

    public Promise<InfiniteStore> registerPageProcessor(Page.PageProcessor processor) {
        return ready().transformAsync((instalce) -> instalce.internalRegisterPageProcessor(processor));
    }

    public Promise<Page> request(Page.PageType type) {
        return ready().transformAsync((ignore) -> {
            return this.processors.parallelStream()
                    .filter((processor) -> processor.type() == type.type())
                    .limit(1)
                    .map(Page.PageProcessor::allocate)
                    .findAny()
                    .orElseGet(() -> {
                        return Promise.exceptional(() -> new RuntimeException("no page allocated for:" + type));
                    });
        });
    }

    protected Promise<InfiniteStore> internalRegisterPageProcessor(Page.PageProcessor processor) {
        if (!this.processors.add(processor)) {
            return Promise.exceptional(() -> new IllegalStateException(("processor already register:" + processor)));
        }

        return this.channel.costly().transformAsync((channel) -> processor.bind(this))
                .transform((binded_processor) -> {
                    Promise.period(() -> {

                        // block and may log each fail period
                        binded_processor.period().catching((throwable) -> {
                            LOGGER.warn("fail when doing period of:" + binded_processor, throwable);
                        }).join();

                    }, TimeUnit.SECONDS.toMillis(1)).logException();
                    return this;
                })
                .catching((throwable) -> {
                    this.processors.remove(processor);
                });
    }

    protected Promise<InfiniteStore> recover() {
        return this.channel.transform((channel) -> {
            long file_size = channel.file().length();
            long total_pages = file_size / Page.SIZE;

            // default processor
            MutablePage processor = new MutablePage();

            Promise<Map.Entry<Long, Boolean>>[] block_statistics = LongStream.range(0, total_pages).parallel()
                    .mapToObj((page_id) -> {
                        return Promise.<Map.Entry<Long, Boolean>>submit(() -> {
                            ByteBuffer local = channel.map(FileChannel.MapMode.READ_ONLY, page_id * Page.SIZE, Page.SIZE);

                            byte used = local.get();
                            if (used <= 0) {
                                // skip not in used
                                return new AbstractMap.SimpleImmutableEntry<>(page_id, false);
                            }

                            Optional.ofNullable(owners.putIfAbsent(page_id, processor)).ifPresent((value) -> {
                                // not accquire
                                throw new IllegalStateException("page:" + page_id + " is expected to be own by:" + processor + " but own by:" + value);
                            });

                            return new AbstractMap.SimpleImmutableEntry<>(page_id, true);
                        });
                    }).toArray(Promise[]::new);

            return Promise.all(block_statistics)
                    .sidekick(() -> {
                        List<Long> live_blocks = Arrays.stream(block_statistics).parallel()
                                .map(Promise::join)
                                .filter(Map.Entry::getValue)
                                .map(Map.Entry::getKey)
                                .collect(Collectors.toList());
                        LOGGER.info("total pages:" + block_statistics.length
                                + " live pages:" + live_blocks.size()
                                + " details:["
                                + live_blocks.stream().parallel()
                                .map(String::valueOf)
                                .collect(Collectors.joining(","))
                                + "]");
                    })
                    .transformAsync((ignore) -> this.internalRegisterPageProcessor(processor));
        }).transformAsync((instance) -> instance);
    }
}

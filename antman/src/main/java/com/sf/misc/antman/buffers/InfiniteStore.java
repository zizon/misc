package com.sf.misc.antman.buffers;

import com.google.common.collect.Maps;
import com.sf.misc.antman.ClosableAware;
import com.sf.misc.antman.Promise;
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
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InfiniteStore {

    public static final Log LOGGER = LogFactory.getLog(InfiniteStore.class);

    protected final Set<Page.PageProcessor> processors;
    protected final Promise<ResizableFileChannel> channel;
    protected final ConcurrentSkipListMap<Long, Page.PageProcessor> owners;

    public InfiniteStore(File storage) {
        this.processors = new ConcurrentSkipListSet<>();
        this.owners = new ConcurrentSkipListMap<>();
        this.channel = Promise.submit(() -> new ResizableFileChannel(storage));
    }

    public void registerPageProcessor(Page.PageProcessor processor) throws Exceptions.BufferException {
        if (!this.processors.add(processor)) {
            throw new Exceptions.DuplicatedPageProcessorException("processor already register:" + processor);
        }

        this.channel.transform((channel) -> processor.bind(channel, owners))
                .catching((throwable) -> {
                    this.processors.remove(processor);
                    LOGGER.error("bind processor fail", throwable);
                })
                .maybe()
                .orElseThrow(() -> new Exceptions.BufferException("regiser page processor fail:" + processor));

        return;
    }

    public Promise<Page> request(Page.PageType type) {
        return Promise.submit(() -> {
            return this.processors.parallelStream()
                    .filter((processor) -> processor.type() == type.type())
                    .findAny()
                    .orElseThrow(() -> new RuntimeException("no processor for page type:" + type));
        }).transform((processor) -> processor.allocate()
                .maybe()
                .orElseThrow(() -> new Exceptions.BufferException("allocate page of type:" + type + " fail"))
        );
    }

    public Promise<InfiniteStore> start() {
        Promise.period(this::doPeriod, TimeUnit.SECONDS.toMillis(1), (throwable) -> {
            LOGGER.warn("do infinite store period fail", throwable);
        }).logException();
        return null;
    }

    protected void doPeriod() {
        processors.parallelStream() //
                .map((processor) -> {
                    return processor.period();
                }) //
                .collect(Collectors.toList()) //
                .forEach((promise) -> {
                    // do not join in common async pool
                    promise.logException().maybe();
                });
    }
}

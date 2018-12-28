package com.sf.misc.antman.buffers;

import com.sf.misc.antman.Promise;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class InvalidPage extends Page.AbstractPageProcessor {

    @Override
    public boolean compatible(Page.PageType type) {
        // intercept all page
        return true;
    }

    @Override
    protected Page initializeBuffer(MappedByteBuffer buffer) {
        throw new IllegalStateException("invalid page can not be initialized");
    }

    @Override
    protected void touchPage(List<Long> offests) {
        Optional.ofNullable(this.channel).ifPresent((channel) -> {
            offests.parallelStream().map((offset) -> {
                return Promise.submit(() -> {
                    return channel.map(FileChannel.MapMode.READ_WRITE, offset * Page.SIZE, Page.SIZE);
                }).transform((buffer) -> {
                    ByteBuffer local = buffer.duplicate();
                    byte used = local.get();
                    if (used > 0) {
                        return Optional.<Long>empty();
                    }

                    // find invalid page
                    return Optional.of(offset);
                });
            }).collect(Collectors.toList()).forEach((promise) -> {
                promise.sidekick((optional) -> {

                    // release page,cast value to null to indicate no one use
                    optional.ifPresent((offset) -> {
                        this.owners.compute(offset, (key, old) -> {
                            return null;
                        });

                        this.invalidateCallback(offset);
                    });
                }).logException();
            });

        });
    }

    @Override
    public byte type() {
        return 0b10;
    }

    protected void invalidateCallback(Long offset) {
    }
}

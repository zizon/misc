package com.sf.misc.antman.simple.client;

import com.sf.misc.antman.Promise;

import java.io.File;
import java.io.IOException;
import java.nio.charset.MalformedInputException;
import java.security.Key;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class IOContext {

    public static interface ProgressListener {
        void onRangeFail(long offset, long size, Throwable reason);

        void onUploadSuccess();

        void onChannelComplete();

        void onProgress(long commited, long failed, long going, long expected);
    }


    public static class Range implements Comparable<Range> {
        protected final long offset;
        protected final long size;

        public Range(long offset, long size) {
            this.offset = offset;
            this.size = size;
        }

        public long offset() {
            return offset;
        }

        public long size() {
            return size;
        }

        @Override
        public int compareTo(Range o2) {
            int first_pass = (int) (this.offset() - o2.offset());
            if (first_pass != 0) {
                return first_pass;
            }

            return (int) (this.size() - o2.size());
        }
    }

    protected final File file;
    protected final UUID stream_id;
    protected final NavigableMap<Range, Promise<?>> range_states;
    protected final long timeout;
    protected final ProgressListener listener;

    public IOContext(File file, UUID stream_id, long chunk, long range_ack_timeout, ProgressListener listener) {
        this.file = file;
        this.stream_id = stream_id;
        this.range_states = new ConcurrentSkipListMap<>();
        this.timeout = range_ack_timeout;
        this.listener = listener;

        // init states
        long total = file.length();
        long chunks = total / chunk
                + (total % chunk == 0 ? 0 : 1);
        LongStream.range(0, chunks).forEach((i) -> {
            long offset = i * chunk;
            long size = Math.min(offset + chunk, total) - offset;
            range_states.put(
                    new Range(offset, size),
                    Promise.promise()
            );
        });
    }

    public List<Range> pending() {
        return this.range_states.entrySet().stream()
                .filter((entry) -> {
                    return !entry.getValue().isDone() || entry.getValue().isCompletedExceptionally();
                })
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }

    public UUID streamID() {
        return stream_id;
    }

    public File file() {
        return file;
    }

    public long timeout() {
        return timeout;
    }

    public void onTimeout(Range range) {
        this.range_states.compute(range, (key, old) -> {
            if (old.isDone()) {
                if (!old.isCompletedExceptionally()) {
                    return old;
                }
            }

            // cancle old
            old.cancel(true);
            return Promise.promise();
        });

        listener.onRangeFail(range.offset(), range.size(), new TimeoutException("stream:" + stream_id + " range:" + range + " timeout:" + timeout));
    }

    public void onFailure(Range range, Throwable reason) {
        this.range_states.compute(range, (key, old) -> {
            old.completeExceptionally(reason);
            return Promise.promise();
        });

        listener.onRangeFail(range.offset(), range.size(), reason);
    }

    public void onCommit(Range range) {
        this.range_states.compute(range, (key, old) -> {
            return Promise.success(null);
        });

        long commited = 0;
        long failed = 0;
        long going = 0;
        long total = file.length();
        for (Map.Entry<Range, Promise<?>> entry : this.range_states.entrySet()) {
            Promise<?> promise = entry.getValue();
            if (promise.isDone()) {
                if (promise.isCompletedExceptionally()) {
                    failed += entry.getKey().size();
                } else {
                    commited += entry.getKey().size();
                }
            } else {
                going += entry.getKey().size();
            }
        }

        listener.onProgress(commited, failed, going, total);
    }

    public void going(Range range, Promise<?> resolve) {
        this.range_states.compute(range, (key, old) -> {
            return resolve;
        });
    }

    public void onSuccess() {
        listener.onUploadSuccess();
    }

    public void onChannelComplete() {
        listener.onChannelComplete();
    }

    public void onCRCNotMatch(long local_crc, long remote_crc) {
        listener.onRangeFail(0, file.length(), new IllegalStateException("stream:" + stream_id + " crc not match,local crc:" + local_crc + " remote crc:" + remote_crc));
    }

    public void onAbortChannel(Throwable reason) {
        this.range_states.entrySet().stream()
                .filter((entry) -> !entry.getValue().isDone())
                .map((entry) -> {
                    // cancel all
                    entry.getValue().cancel(true);
                    return entry.getKey();
                })
                .forEach((range) -> {
                    listener.onRangeFail(range.offset(), range.size(), new IOException("connection abort"));
                });

        listener.onChannelComplete();
    }
}

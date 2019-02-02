package com.sf.misc.antman.simple.client;

import com.sf.misc.antman.Promise;
import com.sf.misc.antman.simple.MemoryMapUnit;
import com.sf.misc.antman.simple.packets.CommitStreamPacket;
import com.sf.misc.antman.simple.packets.Packet;
import com.sf.misc.antman.simple.packets.StreamChunkPacket;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.zip.CRC32;

public interface UploadSession {

    public static Log LOGGER = LogFactory.getLog(UploadSession.class);

    static interface Range extends Comparable<Range> {

        long offset();

        long size();

        long since();

        default int compareTo(Range o2) {
            int first_pass = (int) (this.offset() - o2.offset());
            if (first_pass != 0) {
                return first_pass;
            }

            return (int) (this.size() - o2.size());
        }

    }

    static interface StateListener {

        default void onProgress(long acked, long expected) {
        }

        default void onSuccess(long effected_time) {
        }

        default void onTimeout(Range range, long exipre) {
        }

        default void onUnRecovable(Throwable reason) {
        }

        default boolean onFail(Range range, Throwable cause) {
            return false;
        }
    }

    static UploadSession create(UUID uuid, File file, Promise.PromiseFunction<Packet, Promise<?>> io) {
        return new UploadSession() {
            NavigableMap<Range, Promise<?>> resolved = new ConcurrentSkipListMap<>();
            Set<StateListener> listeners = new CopyOnWriteArraySet<>();
            Promise<?> completion = Promise.promise();
            long crc = calculateCRC();

            @Override
            public UUID stream() {
                return uuid;
            }

            @Override
            public NavigableMap<Range, Promise<?>> resovled() {
                return resolved;
            }

            @Override
            public Promise<?> completion() {
                return completion;
            }

            @Override
            public long crc() {
                return crc;
            }

            @Override
            public File file() {
                return file;
            }

            @Override
            public Set<StateListener> listeners() {
                return listeners;
            }

            @Override
            public Promise.PromiseFunction<Packet, Promise<?>> io() {
                return io;
            }
        };
    }

    UUID stream();

    NavigableMap<Range, Promise<?>> resovled();

    Promise<?> completion();

    default UploadSession start() {
        Stream<Range> ranges = ranges();

        // initialze send
        ranges.parallel().forEach((range) -> {
            Promise<ByteBuffer> content = mmap(range);
            Promise<StreamChunkPacket> packet = content.transform((buffer) -> new StreamChunkPacket(stream(), range.offset(), buffer));

            // write
            Promise<?> wrote = packet.transformAsync((chunk) -> {
                return io().apply(chunk);
            }).addListener(() -> {
                content.sidekick(this::unmap);
            });

            resovled().compute(range, (key, old) -> {
                if (old == null) {
                    return wrote;
                }

                //wrote.cancel(true);
                if (old.isDone()) {
                    wrote.cancel(true);
                    return old;
                } else {
                    old.cancel(true);
                    return wrote;
                }
            });
        });

        // health check
        Promise<?> period = Promise.period(this::periodWork, timeout());

        // internal listener
        this.addStateListener(new StateListener() {
            @Override
            public void onSuccess(long effected_time) {
                period.cancel(true);
            }

            @Override
            public void onUnRecovable(Throwable reason) {
                period.cancel(true);
                resovled().values().parallelStream().forEach((promise) -> promise.cancel(true));
            }
        });

        // complection callback
        completion().sidekick(() -> {
            long end_time = System.currentTimeMillis();
            long start_time = resovled().keySet().parallelStream()
                    .map(Range::since)
                    .filter((x) -> x > 0)
                    .min(Long::compareTo)
                    .orElse(end_time);

            listeners().parallelStream().forEach((listener) -> {
                listener.onSuccess(end_time - start_time);
            });
        }).catching((throwable) -> {
            if (!completion().isCancelled()) {
                listeners().parallelStream().forEach((listener) -> {
                    listener.onUnRecovable(throwable);
                });
            }
        }).logException();

        return this;
    }

    default void periodWork() {
        // cancel timeout
        cancelTimeout();

        // restart failed
        retry();

        // update state
        updateProgress();
    }

    default void commit(Range range) {
        // ack commit
        resovled().compute(range, (key, old) -> {
            if (old != null && !old.isDone()) {
                old.cancel(true);
            }

            return Promise.success(null);
        });

        periodWork();
    }

    default void cancelTimeout() {
        long now = System.currentTimeMillis();
        long timeout = timeout();
        resovled().entrySet().parallelStream()
                .filter((entry) -> {
                    return !entry.getValue().isDone();
                })
                .filter((entry) -> {
                    return now - entry.getKey().since() > timeout;
                })
                .forEach((entry) -> {
                    listeners().parallelStream().forEach((listener) -> {
                        listener.onTimeout(entry.getKey(), now - entry.getKey().since() - timeout);
                    });

                    sendRange(entry.getKey());
                });
    }

    default void retry() {
        resovled().entrySet().parallelStream()
                .filter((entry) -> {
                    return entry.getValue().isCompletedExceptionally() && !entry.getValue().isCancelled();
                })
                .forEach((entry) -> {
                    Range range = entry.getKey();
                    entry.getValue().catching((throwable) -> {
                        boolean retry = listeners().parallelStream().map((listener) -> listener.onFail(range, throwable))
                                .reduce(Boolean::logicalOr)
                                .orElse(false);
                        if (!retry) {
                            return;
                        }

                        sendRange(range);
                    });
                });
    }

    default Promise<?> sendRange(Range range) {
        Promise<ByteBuffer> content = mmap(range);
        Promise<StreamChunkPacket> packet = content.transform((buffer) -> new StreamChunkPacket(stream(), range.offset(), buffer));

        // write
        Promise<?> wrote = packet.transformAsync((chunk) -> {
            return io().apply(chunk);
        }).addListener(() -> {
            content.sidekick(this::unmap);
        });

        // replace
        return resovled().compute(range, (key, old) -> {
            if (old != null) {
                old.cancel(true);
            }

            return wrote;
        });
    }

    default void updateProgress() {
        long acked = resovled().entrySet().parallelStream()
                .filter((entry) -> entry.getValue().isDone() && !entry.getValue().isCompletedExceptionally())
                .map(Map.Entry::getKey)
                .collect(Collectors.summingLong(Range::size));

        // progress
        listeners().parallelStream().forEach((listener) -> {
            listener.onProgress(acked, expected());
        });


        // all done
        if (acked == expected()) {
            //send crc
            io().apply(new CommitStreamPacket(stream(), expected(), crc())).logException();
        }
    }

    long crc();

    default long calculateCRC() {
        long expected = expected();
        long chunk = 1024 * 1024 * 128;
        CRC32 crc = new CRC32();
        long split = (expected / chunk)
                + (expected % chunk == 0 ? 0 : 1);

        LongStream.range(0, split).sequential()
                .forEach((i) -> {
                    long offset = i * chunk;
                    long size = Math.min(offset + chunk, expected) - offset;
                    mmap(newRange(offset, size)).transform((buffer) -> {
                        crc.update(buffer);
                        unmap(buffer);
                        return null;
                    }).join();
                });

        return crc.getValue();
    }

    default long expected() {
        return file().length();
    }

    File file();

    Set<StateListener> listeners();

    default MemoryMapUnit mmu() {
        return MemoryMapUnit.shared();
    }

    default void addStateListener(StateListener state_listener) {
        Optional.ofNullable(state_listener).ifPresent((listener) -> {
            listeners().add(listener);
            updateProgress();
        });
    }

    default Promise<ByteBuffer> mmap(Range range) {
        return mmu().map(file(), range.offset(), range.size());
    }

    default void unmap(ByteBuffer buffer) {
        mmu().unmap(buffer);
    }

    default Stream<Range> ranges() {
        long expected = expected();
        long batch = batch();
        long chunks = (expected / batch)
                + ((expected % batch == 0) ? 0 : 1);

        return LongStream.range(0, chunks).parallel()
                .mapToObj((i) -> {
                    long offset = i * batch;
                    return newRange(offset, Math.min(offset + batch, expected) - offset);
                });
    }

    default long batch() {
        return 64 * 1024;
    }

    default long netIOBytesPerSecond() {
        return 100 * 1024;
    }

    default long diskIOBytesPerSecond() {
        return 10 * 1024 * 1024;
    }

    default long scheduleCostPerChunk() {
        return 10;
    }

    default long driftDelay() {
        return 1;
    }

    default long timeout() {
        long bytes_rate = netIOBytesPerSecond() * 1000
                + diskIOBytesPerSecond() * 1000;
        long io_cost = expected() / bytes_rate;
        long scheudle_cost = scheduleCostPerChunk() * batch();
        long theory_cost = io_cost + scheudle_cost + driftDelay();
        return theory_cost;
    }

    default Range newRange(long offset, long size) {
        return newRange(offset, size, System.currentTimeMillis());
    }

    default Range newRange(long offset, long size, long since) {
        return new Range() {
            @Override
            public long offset() {
                return offset;
            }

            @Override
            public long size() {
                return size;
            }

            @Override
            public long since() {
                return since;
            }

            @Override
            public String toString() {
                return "offset:" + offset + " size:" + size + " since:" + since;
            }

        };
    }

    Promise.PromiseFunction<Packet, Promise<?>> io();

}
package com.sf.misc.antman.simple.client;

import com.sf.misc.antman.Promise;
import com.sf.misc.antman.simple.CRC;
import com.sf.misc.antman.simple.MemoryMapUnit;
import com.sf.misc.antman.simple.packets.CommitStreamPacket;
import com.sf.misc.antman.simple.packets.StreamChunkPacket;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeoutException;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class UploadChannel {

    public static final Log LOGGER = LogFactory.getLog(UploadChannel.class);

    @FunctionalInterface
    public static interface ProgressListener {
        void onProgress(long commited, long failed, long going, long total);
    }

    @FunctionalInterface
    public static interface FailureListener {
        void onUploadFail(File file, UUID stream_id, Throwable reason);
    }

    @FunctionalInterface
    public static interface SuccessListener {
        void onSuccess(File file, UUID stream_id);
    }

    @FunctionalInterface
    public static interface RetryPolicy {
        boolean shouldRetry(long offset, long size, int had_retrid, Throwable fail_reason);
    }

    protected final IOHandler handler;

    protected UploadChannel(IOHandler handler) {
        this.handler = handler;
    }

    public void upload(File file, UUID stream_id, ProgressListener progress, FailureListener failure, RetryPolicy retry_policy, SuccessListener success) {
        this.handler.connect().catching((throwable) -> {
            failure.onUploadFail(file, stream_id, throwable);
        }).sidekick(() -> {
            establised(file, stream_id, progress, failure, retry_policy, success);
        });
    }

    protected void establised(File file, UUID stream_id, ProgressListener progress, FailureListener failure, RetryPolicy retry_policy, SuccessListener success) {
        long chunk = chunk();
        long total = file.length();
        long chunks = total / chunk() +
                ((file.length() % chunk() == 0) ? 0 : 1);

        Stream<IOContext.Range> ranges = LongStream.range(0, chunks)
                .mapToObj((i) -> new IOContext.Range(i * chunk, Math.min(chunk, total - i * chunk)));

        // generator
        Promise.PromiseFunction<IOContext.Range, Promise<?>> packet_write_generator = packetWriteGenearator(file, stream_id);

        // retryid map
        NavigableMap<IOContext.Range, Integer> retried = new ConcurrentSkipListMap<>();

        // then send
        NavigableMap<IOContext.Range, Promise<?>> sent = new ConcurrentSkipListMap<>();
        ranges.forEach((range) -> {
            // retryied map
            retried.put(range, 0);
            sent.put(range, packet_write_generator.apply(range));
        });


        // add timeout
        sent.keySet().stream().forEach((range) -> {
            setupTimeout(range, retried, sent, packet_write_generator, retry_policy);
        });

        // try read ack
        Promise<?> sent_done = Promise.costly(() -> {
            for (Optional<IOContext.Range> range = handler.read(); range != null; range = handler.read()) {
                if (!range.isPresent()) {
                    //EOF
                    break;
                }

                sent.compute(range.get(), (key, value) -> {
                    value.complete(null);
                    return Promise.success(null);
                });

                // do progress
                progress(progress, sent);

                // shoudl terminate?
                boolean any_left = sent.values().stream().filter((promise) -> !promise.isDone())
                        .findAny()
                        .map((ignore) -> true)
                        .orElse(false);

                if (!any_left) {
                    break;
                }
            }
        }).catching((throwable) -> {
            failure.onUploadFail(file, stream_id, throwable);
        });

        // crc
        Promise<Long> my_crc = sent_done.transformAsync((ignore) -> mmu().map(file, 0, file.length())
                .transform((page) -> CRC.crc(page)));

        // send commit
        Promise<?> send_crc = my_crc.transformAsync((crc) -> handler.write(new CommitStreamPacket(stream_id, file.length(), crc)));

        // read commit message
        send_crc.transformAsync((ignore) -> my_crc).costly().transform((crc) -> {
            long remote_crc = handler.readCRC();
            if (remote_crc != crc) {
                failure.onUploadFail(file, stream_id, new IllegalStateException("file:" + file + " crc not match,local:" + crc + " remote:" + remote_crc));
                return null;
            }

            // matched
            // done
            progress((commited, failed, going, size) -> {
                if (commited == size) {
                    success.onSuccess(file, stream_id);
                } else {
                    failure.onUploadFail(file, stream_id, new IllegalStateException("file:" + file + " partital commit,expecte:" + size + " but got:" + commited));
                }
            }, sent);
            return null;
        }).addListener(() -> {
            handler.close();
        }).logException();
    }

    protected Promise.PromiseFunction<IOContext.Range, Promise<?>> packetWriteGenearator(File file, UUID stream_id) {
        return (range) -> {
            // generate page
            Promise<ByteBuffer> page = mmu().map(file, range.offset(), range.size());

            // generate packet
            Promise<StreamChunkPacket> packet = page.transform((buffer) -> {
                return new StreamChunkPacket(stream_id, range.offset(), buffer);
            });

            // sent packet
            Promise<?> sent_packet = packet.transformAsync((content) -> {
                return handler.write(content);
            });

            // clean up
            Promise.all(sent_packet, packet).addListener(() -> {
                mmu().unmap(page.join());
            });
            return sent_packet;
        };
    }

    protected void progress(ProgressListener listener, NavigableMap<IOContext.Range, Promise<?>> sent) {
        long commited = 0;
        long total = 0;
        long failed = 0;
        long going = 0;
        for (Map.Entry<IOContext.Range, Promise<?>> entry : sent.entrySet()) {
            IOContext.Range range = entry.getKey();
            Promise<?> promise = entry.getValue();

            // total
            total += range.size();

            if (promise.isDone()) {
                if (promise.isCompletedExceptionally()) {
                    failed += range.size();
                } else {
                    commited += range.size();
                }
            } else {
                going += range.size();
            }
        }

        listener.onProgress(commited, failed, going, total);
    }

    protected void setupTimeout(IOContext.Range range, NavigableMap<IOContext.Range, Integer> retried, NavigableMap<IOContext.Range, Promise<?>> sent, Promise.PromiseFunction<IOContext.Range, Promise<?>> packet_write_generator, RetryPolicy retry_policy) {
        // add timeout
        sent.compute(range, (key, old) -> {
            // wrap write with timeout
            return old.timeout(() -> {
                // when timeout
                int count = retried.compute(range, (index, counter) -> {
                    counter++;
                    return counter;
                }) - 1;

                // retry count
                boolean retry = retry_policy.shouldRetry(
                        range.offset(),
                        range.size(),
                        count,
                        new TimeoutException("range:" + key + " tiemout:" + timeout())
                );

                // retry
                if (retry) {
                    // write packet
                    sent.put(range, packet_write_generator.apply(range));

                    // add timeout
                    setupTimeout(range, retried, sent, packet_write_generator, retry_policy);
                }

                return null;
            }, timeout());

        });

        LOGGER.info("timeout:" + timeout());
    }

    protected MemoryMapUnit mmu() {
        return MemoryMapUnit.shared();
    }

    protected long chunk() {
        return 4 * 1024 * 1024;
    }

    protected long timeout() {
        return (chunk() / (100 * 1024)) * 1000;
    }
}

package com.sf.misc.antman.io;

import com.sf.misc.antman.Promise;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ByteStream {

    public static final Log LOGGER = LogFactory.getLog(ByteStream.class);

    protected final FileStore.MMU mmu;

    @FunctionalInterface
    public static interface TransferCallback {

        public Promise<Void> requsetTransfer(ByteBuffer provider, long offset) throws Throwable;
    }

    public static class TransferReqeust {

        protected final StreamContext context;
        protected final long stream_offset;
        protected final long length;
        protected final TransferCallback callback;


        public TransferReqeust(StreamContext context, long stream_offset, long length, TransferCallback callback) {
            this.context = Optional.of(context).get();
            this.stream_offset = stream_offset;
            this.length = length;
            this.callback = Optional.of(callback).get();
        }
    }

    public static class StreamContext {

        protected final UUID uuid;

        protected final NavigableSet<StreamBlock> blocks;

        public StreamContext(UUID uuid) {
            this.uuid = Optional.of(uuid).get();
            this.blocks = new ConcurrentSkipListSet<>((left, right) -> {
                int stream_match = left.stream_id.compareTo(right.stream_id);
                if (stream_match != 0) {
                    throw new IllegalStateException("stream not match, left:" + left + " right:" + right);
                }

                int offset_match = Long.compare(left.offset, right.offset);
                if (offset_match != 0) {
                    return offset_match;
                }

                // smaller first
                return Long.compare(left.length, right.length);
            });
        }

        public UUID uuid() {
            return this.uuid;
        }

        public Promise<Void> drop() {
            return this.blocks.parallelStream()
                    .map((stream_block) -> {
                        return stream_block.block.free();
                    })
                    .collect(Promise.collector());
        }

        public Promise<StreamContext> addBlock(StreamBlock block) {
            if (block == null) {
                return Promise.success(this);
            } else if (block.stream_id.compareTo(uuid) != 0) {
                throw new IllegalStateException("block uuid:" + block.stream_id + " not match context stream id:" + uuid);
            } else if (!this.blocks.add(block)) {
                throw new IllegalStateException("block already add:" + block);
            }
            return Promise.success(this);
        }

        public Promise<?> tranferTo(ByteBuffer buffer) {
            return this.blocks.parallelStream().map((block) -> {
                long offset = block.offset;
                long length = block.length;

                // slice part
                ByteBuffer write_to = buffer.duplicate();
                write_to.position((int) offset);
                write_to.limit((int) (write_to.position() + length));
                ByteBuffer final_write = write_to.slice();

                return block.zone().transform((zone) -> {
                    zone.limit((int) length);
                    final_write.put(zone);
                    return null;
                });
            }).collect(Promise.collector());
        }

        public Stream<Promise<ByteBuffer>> contents() {
            return StreamSupport.stream(new Iterable<Promise<ByteBuffer>>() {
                Iterator<StreamBlock> delegate = blocks.parallelStream()
                        .sorted(Comparator.comparing((stream_block) -> stream_block.offset))
                        .iterator();

                @Override
                public Iterator<Promise<ByteBuffer>> iterator() {
                    return new Iterator<Promise<ByteBuffer>>() {
                        long expected_offset = 0;

                        @Override
                        public boolean hasNext() {
                            return delegate.hasNext();
                        }

                        @Override
                        public Promise<ByteBuffer> next() {
                            StreamBlock block = delegate.next();

                            // is sutiable block
                            if (block.offset > expected_offset) {
                                throw new IllegalStateException("out of range,may be a gap in block stream, expected:" + expected_offset + " but got far:" + block.offset);
                            }

                            // calculate in block offset
                            long in_block_offset = expected_offset - block.offset;

                            // advance
                            expected_offset = block.offset + block.length;

                            return block.zone().transform((buffer) -> {
                                try {
                                    buffer.position((int) in_block_offset);
                                    return buffer.slice();
                                } catch (Throwable e) {
                                    throw new RuntimeException("fail at position:" + in_block_offset, e);
                                }
                            });
                        }
                    };
                }
            }.spliterator(), true);
        }

        @Override
        public String toString() {
            return "stream:" + uuid + " with blocks:" + blocks.size();
        }
    }

    protected static class StreamBlock {

        protected final FileStore.Block block;
        protected final UUID stream_id;
        protected final long offset;
        protected final long length;

        public StreamBlock(UUID stream_id, long stream_offset, long length, FileStore.Block block) {
            this.stream_id = stream_id;
            this.offset = stream_offset;
            this.length = length;
            this.block = Optional.of(block).get();

            if (!block.isUsed()) {
                throw new IllegalStateException("block is not in used:" + this.block);
            }
        }

        public Promise<ByteBuffer> zone() {
            return this.block.zone().transform((zone) -> {
                // skip uuid
                long high = zone.getLong();
                long low = zone.getLong();

                UUID deserialized = new UUID(high, low);
                if (deserialized.compareTo(this.stream_id) != 0) {
                    throw new IllegalStateException("deserialize stream id not match, expected:" + this.stream_id + " got:" + deserialized);
                }

                // skip offset
                long offset = zone.getLong();
                if (offset != this.offset) {
                    throw new IllegalStateException("deserialize stream offset not match,expected:" + this.offset + " got:" + offset);
                }

                // lenght
                long length = zone.getLong();
                if (length != this.length) {
                    throw new IllegalStateException("deserialize stream block length not match,expected:" + this.length + " got:" + length);
                }

                zone.limit((int) (zone.position() + length));
                return zone.slice();
            });
        }

        @Override
        public String toString() {
            return "stream block:" + stream_id + " offset:" + offset + " length:" + length + " block:" + block;
        }
    }

    public static Promise<NavigableSet<StreamContext>> loadFromMMU(FileStore.MMU mmu) {
        ConcurrentMap<UUID, StreamContext> streams = new ConcurrentHashMap<>();

        return mmu.dangle().parallelStream()
                .map((block) -> {
                    return block.zone().transform((zone) -> {
                        // find uuid
                        long most_significan = zone.getLong();
                        long least_significan = zone.getLong();

                        // skip uuid
                        long high = zone.getLong();
                        long low = zone.getLong();

                        // faulted page
                        if (high == 0 && low == 0) {
                            mmu.release(block);
                            return null;
                        }

                        UUID deserialized = new UUID(high, low);

                        // skip offset
                        long offset = zone.getLong();

                        // length
                        long length = zone.getLong();

                        StreamBlock stream_block = new StreamBlock(deserialized, offset, length, block);

                        // find context
                        StreamContext context = streams.get(deserialized);
                        if (context == null) {
                            context = new StreamContext(deserialized);
                            streams.putIfAbsent(deserialized, context);
                            context = streams.get(deserialized);
                        }

                        context.addBlock(stream_block);

                        return null;
                    });
                })
                .collect(Promise.collector())
                .transform((ignore) -> {
                    NavigableSet<StreamContext> context_set = new ConcurrentSkipListSet<>((left, right) -> {
                        return left.uuid.compareTo(right.uuid);
                    });

                    context_set.addAll(streams.values());
                    return context_set;
                });
    }

    public ByteStream(FileStore.MMU mmu) {
        this.mmu = Optional.of(mmu).get();
    }

    public Promise<ByteStream> transfer(TransferReqeust reqeust) {
        long content_size = reqeust.length;
        long block_size = FileStore.Block.capacity();
        long unit_overhead = Long.BYTES + Long.BYTES // uuid
                + Long.BYTES // stream offset
                + Long.BYTES // content length
                ;

        long usable_per_block = block_size - unit_overhead;

        // calculate how many block to request
        long need_blocks = content_size / usable_per_block
                + ((content_size % usable_per_block) > 0 ? 1 : 0);

        TransferCallback transfer_callback = reqeust.callback;
        UUID stream_id = reqeust.context.uuid;
        long base_offset = reqeust.stream_offset;

        return LongStream.range(0, need_blocks).parallel()
                .mapToObj((content_block_id) -> {
                    // requeat memroy block
                    return this.mmu.request().transformAsync((block) -> {
                        // write cotent to block
                        return block.write((provided) -> {
                            ByteBuffer writable = provided.duplicate();

                            if (writable.remaining() != unit_overhead + usable_per_block) {
                                throw new IllegalStateException("request block size incorrect, expected:" + (unit_overhead + usable_per_block) + " got:" + writable.remaining());
                            }

                            // write uuid
                            writable.putLong(stream_id.getMostSignificantBits());
                            writable.putLong(stream_id.getLeastSignificantBits());

                            // write stream offset
                            long relative_offset = content_block_id * usable_per_block;
                            long offset = base_offset + relative_offset;
                            writable.putLong(offset);

                            // write length
                            long expected_end = relative_offset + usable_per_block;
                            if (expected_end > content_size) {
                                expected_end = content_size;
                            }
                            long length = expected_end - relative_offset;
                            writable.putLong(length);

                            // slice writable
                            writable.limit((int) (writable.position() + length));
                            ByteBuffer slice = writable.slice();

                            return transfer_callback.requsetTransfer(slice, content_block_id * usable_per_block)
                                    .transform((ignore) -> {
                                        // slice should had nothing left
                                        if (slice.hasRemaining()) {
                                            throw new IllegalStateException("slice should had be consumed,but left:" + slice + " transfer request:" + reqeust);
                                        }

                                        // add to context
                                        reqeust.context.addBlock(new StreamBlock(
                                                reqeust.context.uuid,
                                                offset,
                                                length,
                                                block
                                        ));

                                        return null;
                                    });
                        }).catching((throwable) -> {
                            // wirte fail,release buffer
                            mmu.release(block).logException();
                        });
                    });
                })
                .collect(Promise.collector())
                .transform((ignore) -> this);
    }
}

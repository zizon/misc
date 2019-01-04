package com.sf.misc.antman;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.zip.CRC32;

public class TestByteStream {

    public static final Log LOGGER = LogFactory.getLog(TestByteStream.class);

    protected Promise<Long> crc(File target) {
        return ClosableAware.wrap(() -> FileChannel.open(target.toPath(), StandardOpenOption.READ))
                .transform((mmap) -> {
                    return mmap.map(FileChannel.MapMode.READ_ONLY, 0, target.length());
                })
                .transform((buffer) -> {
                    CRC32 crc = new CRC32();
                    crc.update(buffer);
                    return crc.getValue();
                });
    }

    @Test
    public void testLoadFromMMU() {
        File storage = new File("__storage__");
        File input_file = new File("large_exec_file.exe");

        Promise<Long> origin = crc(input_file);

        Promise<FileStore.MMU> mmu_promise = new FileStore(storage).mmu();

        // recover byte stream
        Promise<List<ByteStream.StreamContext>> load_from_mmu = mmu_promise.transformAsync(ByteStream::loadFromMMU)
                .transform((contexts) -> {
                    return contexts.parallelStream().parallel()
                            .collect(Collectors.toList());
                });

        LOGGER.info("crc:" + input_file + " is " + origin.join());
        LOGGER.info("stream contexts:\n" + load_from_mmu.join().parallelStream()
                .map((context) -> {
                    return "load context:" + context;
                })
                .collect(Collectors.joining("\n"))
        );

        mmu_promise.transform((mmu) -> {
            LOGGER.info("reopen storage:" + storage);
            LOGGER.info("estimate free:" + mmu.esitamteFree());
            LOGGER.info("estiamte used:" + mmu.estiamteUsed());
            return null;
        }).join();
    }

    @Test
    public void testReadWrite() {
        File storage = new File("__storage__");
        storage.delete();

        File input_file = new File("large_exec_file.exe");

        Promise<Long> origin = crc(input_file);

        Promise<FileStore.MMU> mmu_promise = new FileStore(storage).mmu();

        UUID stream_id = UUID.nameUUIDFromBytes(input_file.getName().getBytes());
        ByteStream.StreamContext context = new ByteStream.StreamContext(stream_id);
        LOGGER.info("using stream context:" + context);

        Promise<Void> write_done = mmu_promise.transformAsync((mmu) -> {
            // make file to 4m slice
            long max = input_file.length();
            long size = max;
            long unit_size = 1 * 1024 * 1024; //4m

            long slice = size / unit_size
                    + (size % unit_size > 0 ? 1 : 0);

            ByteStream stream = new ByteStream(mmu);
            FileChannel channel = FileChannel.open(input_file.toPath(), StandardOpenOption.READ);

            return Promise.all(
                    LongStream.range(0, slice).parallel()
                            .mapToObj((slice_id) -> {
                                long map_offset = slice_id * unit_size;
                                long expected_limit = map_offset + unit_size;
                                if (expected_limit > max) {
                                    expected_limit = max;
                                }

                                AtomicLong start = new AtomicLong(System.nanoTime());
                                return Promise.success(expected_limit - map_offset)
                                        .transformAsync((expected_size) -> {
                                            // map chunk
                                            long end = System.nanoTime();
                                            double cost = end - start.get();
                                            //LOGGER.info("immedia commit cost:" + cost / TimeUnit.MILLISECONDS.toNanos(1));
                                            start.set(System.nanoTime());

                                            return stream.write(context, map_offset, channel.map(
                                                    FileChannel.MapMode.READ_ONLY,
                                                    map_offset,
                                                    expected_size));
                                        })
                                        .transform((ignore) -> {
                                            long end = System.nanoTime();
                                            double cost = end - start.get();
                                            //LOGGER.info("cost:" + cost / TimeUnit.MILLISECONDS.toNanos(1));
                                            return null;
                                        });
                            })
            ).sidekick(() -> channel.close()).catching((ignore) -> channel.close());
        });

        Promise<Long> generated = write_done.transform((ignore) -> {
            LOGGER.info("calcualte crc...");
            CRC32 crc = new CRC32();
            context.contents()
                    //.parallel()
                    .map(Promise::join)
                    .sequential()
                    .forEach((buffer) -> {
                        crc.update(buffer);
                    });

            return crc.getValue();
        });

        LOGGER.info("origin crc:" + origin.join());
        LOGGER.info("generated crc:" + generated.join());
        Assert.assertEquals(origin.join(), generated.join());
    }


    @Test
    public void testLog() {
        LOGGER.info("ok?");
        try {
            LOGGER.info("excetpion?");
            throw new RuntimeException("test exception");
        } catch (Throwable e) {
            LOGGER.error("fail", e);
        }
    }
}

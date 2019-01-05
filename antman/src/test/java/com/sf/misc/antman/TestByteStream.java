package com.sf.misc.antman;

import com.sf.misc.antman.benchmark.Tracer;
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
        Tracer total_cost = new Tracer("total").start();
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

            FileChannel channel = FileChannel.open(input_file.toPath(), StandardOpenOption.READ);
            ByteStream stream = new ByteStream(mmu);

            return stream.transfer(
                    new ByteStream.TransferReqeust(
                            context,
                            0,
                            size,
                            (writable, offset) -> {
                                long length = writable.remaining();

                                return Promise.light(
                                        () -> channel.map(FileChannel.MapMode.READ_ONLY, offset, length)
                                ).<Void>transform((readed) -> {
                                    writable.put(readed);
                                    return null;
                                }).catching((throwable) -> {
                                    LOGGER.error("size:" + size + " offset:" + offset + " length:" + length, throwable);
                                });
                            })
            ).addListener(() -> channel.close())
                    .transform((ignore) -> null)
                    ;
        });

        Promise<Long> generated = write_done.transform((ignore) -> {
            LOGGER.info("calcualte crc...");
            CRC32 crc = new CRC32();
            context.contents()
                    .map(Promise::join)
                    .sequential()
                    .forEach((buffer) -> {
                        crc.update(buffer);
                    });

            return crc.getValue();
        });

        LOGGER.info("origin crc:" + origin.join());
        LOGGER.info("generated crc:" + generated.join());
        LOGGER.info(total_cost.stop());
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

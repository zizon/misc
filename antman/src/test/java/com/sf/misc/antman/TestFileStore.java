package com.sf.misc.antman;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.zip.CRC32;

public class TestFileStore {

    public static final Log LOGGER = LogFactory.getLog(TestFileStore.class);

    @Test
    public void testMMU() {
        File storage = new File("__storage__");
        storage.delete();

        int allocat = 100;
        Promise<Void> allcoate = new FileStore(storage).mmu() //
                .transformAsync((mmu) -> {
                    LOGGER.info("init mmu ok");

                    return Promise.all(
                            IntStream.range(0, allocat).parallel()
                                    .mapToObj((request_id) -> mmu.request())
                    ).transform((ignore) -> {
                        LOGGER.info("allocate :" + allocat + " blocks");

                        LOGGER.info("estimate free:" + mmu.esitamteFree());
                        LOGGER.info("estiamte used:" +  mmu.estiamteUsed());
                        return null;
                    });
                });

        allcoate.transformAsync((ignore) -> new FileStore(storage).mmu())
                .transform((mmu) -> {
                    LOGGER.info("reopen storage:" + storage);
                    LOGGER.info("estimate free:" + mmu.esitamteFree());
                    LOGGER.info("estiamte used:" +  mmu.estiamteUsed());
                    LOGGER.info("used blocks:\n" + mmu.dangle.parallelStream()
                            .map((block) -> {
                                return "block:" + block;
                            }).collect(Collectors.joining("\n")));

                    Assert.assertEquals(mmu.dangle.size(), allocat);
                    return null;
                }).join();
    }
}

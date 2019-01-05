package com.sf.misc.antman.io;

import com.sf.misc.antman.Promise;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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

                    return IntStream.range(0, allocat).parallel()
                            .mapToObj((request_id) -> mmu.request())
                            .collect(Promise.collector())
                            .transform((ignore) -> {
                                LOGGER.info("allocate :" + allocat + " blocks");

                                LOGGER.info("estimate free:" + mmu.esitamteFree());
                                LOGGER.info("estiamte used:" + mmu.estiamteUsed());
                                return null;
                            });
                });

        allcoate.transformAsync((ignore) -> new FileStore(storage).mmu())
                .transform((mmu) -> {
                    LOGGER.info("reopen storage:" + storage);
                    LOGGER.info("estimate free:" + mmu.esitamteFree());
                    LOGGER.info("estiamte used:" + mmu.estiamteUsed());
                    LOGGER.info("used blocks:\n" + mmu.dangle.parallelStream()
                            .map((block) -> {
                                return "block:" + block;
                            }).collect(Collectors.joining("\n")));

                    Assert.assertEquals(mmu.dangle.size(), allocat);
                    return null;
                }).join();
    }
}

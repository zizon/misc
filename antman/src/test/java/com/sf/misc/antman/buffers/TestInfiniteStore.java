package com.sf.misc.antman.buffers;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;

public class TestInfiniteStore {

    public static final Log LOGGER = LogFactory.getLog(TestInfiniteStore.class);

    @Test
    public void test() throws Throwable {
        Semaphore semaphore = new Semaphore(1);
        semaphore.acquire();
        File store_dir = new File("__storage__");
        store_dir.mkdirs();

        InfiniteStore store = new InfiniteStore(store_dir);

        store.registerPageProcessor(new InvalidPage(){
            protected void invalidateCallback(long offset){
                LOGGER.info("invalidate offset:" + offset);
            }
        });
        store.registerPageProcessor(new MutablePage());

        store.request(new MutablePage()).sidekick((page) -> {
            LOGGER.info("allocate page:" + page);
            semaphore.release();
        }).maybe();

        semaphore.acquire();
        LOGGER.info("done");
    }
}

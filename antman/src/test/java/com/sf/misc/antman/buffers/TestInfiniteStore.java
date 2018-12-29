package com.sf.misc.antman.buffers;

import com.sf.misc.antman.Promise;
import com.sf.misc.antman.buffers.processors.ExamingPage;
import com.sf.misc.antman.buffers.processors.MutablePage;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.IntStream;

public class TestInfiniteStore {

    public static final Log LOGGER = LogFactory.getLog(TestInfiniteStore.class);

    @Test
    public void test() {
        int paralle = 11;
        CountDownLatch count = new CountDownLatch(paralle * 2);
        File store_dir = new File("__storage__");
        if (store_dir.isDirectory()) {
            store_dir.delete();
        }

        InfiniteStore store = new InfiniteStore(store_dir).ready()
                //.transformAsync((instance) -> instance.registerPageProcessor(new ExamingPage()))
                .join();

        Promise.all(IntStream.range(0, 11).parallel().mapToObj((i) -> {
                    return store.request(new MutablePage()).sidekick((page) -> {
                        LOGGER.info("allocate page:" + page.buffer + " reclaim it");
                    });
                }).toArray(Promise[]::new)
        ).logException().join();


        LOGGER.info("done");
    }
}

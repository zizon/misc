package com.sf.misc.async;

import io.airlift.log.Logger;
import org.junit.Test;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public class TestPromises {

    public static final Logger LOGGER = Logger.get(TestPromises.class);

    public void testRetry() throws Throwable {
        AtomicInteger countdown = new AtomicInteger(3);
        Promises.retry(() -> {
            LOGGER.info("call once");
            if (countdown.decrementAndGet() == 0) {
                return Optional.of(1);
            }
            return Optional.empty();
        }).unchecked();
    }

    @Test
    public void testScheudle() throws Throwable {
        Promises.schedule(() -> {
            LOGGER.info("tick...");
        }, TimeUnit.SECONDS.toMillis(5), false).logException();

        Promises.schedule(() -> {
            LOGGER.info("tick 2...");
            throw new RuntimeException("tick 2");
        }, TimeUnit.SECONDS.toMillis(5), true).logException();

        LockSupport.park();
    }

}

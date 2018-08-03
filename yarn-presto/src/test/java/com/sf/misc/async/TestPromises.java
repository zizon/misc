package com.sf.misc.async;

import io.airlift.log.Logger;
import org.junit.Test;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class TestPromises {

    public static final Logger LOGGER = Logger.get(TestPromises.class);

    @Test
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
}

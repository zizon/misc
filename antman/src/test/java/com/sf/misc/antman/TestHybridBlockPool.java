package com.sf.misc.antman;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import java.nio.MappedByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;

public class TestHybridBlockPool {

    public static final Log LOGGER = LogFactory.getLog(TestHybridBlockPool.class);

    protected void showStorage() {
        LOGGER.info("show storage------------------------------");
        Arrays.stream(HybridBlockPool.STORAGE.listFiles()).forEach((file) -> {
            LOGGER.info("storage:" + file.toURI());
        });
    }
}

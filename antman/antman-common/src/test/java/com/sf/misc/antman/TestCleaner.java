package com.sf.misc.antman;

import com.sf.misc.antman.simple.MemoryMapUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.concurrent.locks.LockSupport;

public class TestCleaner {

    public static final Log LOGGER = LogFactory.getLog(TestCleaner.class);

    @Test
    public void test() {
        Cleaner.create(ByteBuffer.allocateDirect(1024), () -> {
            LOGGER.info("ok");
        });

        System.gc();
    }

}

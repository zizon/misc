package com.sf.misc.antman;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

public class TestMMap {

    public static final Log LOGGER = LogFactory.getLog(TestMMap.class);

    @Test
    public void testBit(){
        long a = 0x0001000000000000l;
        LOGGER.info(Long.numberOfLeadingZeros(a));
    }
}

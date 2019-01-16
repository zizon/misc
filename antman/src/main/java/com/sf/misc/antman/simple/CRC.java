package com.sf.misc.antman.simple;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

public class CRC {

    public static long crc(ByteBuffer buffer) {
        CRC32 crc = new CRC32();
        crc.update(buffer.duplicate());
        return crc.getValue();
    }
}

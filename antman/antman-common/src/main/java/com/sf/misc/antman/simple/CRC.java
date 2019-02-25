package com.sf.misc.antman.simple;

import com.sf.misc.antman.Promise;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.stream.LongStream;
import java.util.zip.CRC32;

public class CRC {

    public static long crc(ByteBuffer buffer) {
        CRC32 crc = new CRC32();
        crc.update(buffer.duplicate());
        return crc.getValue();
    }

    public static Promise<Long> crc(File file) {
        long total = file.length();
        long slice = 1 * 1024 * 1024 * 100; //100m
        long partition = total / slice
                + (total % slice == 0 ? 0 : 1);
        CRC32 crc = new CRC32();

        Promise<Long> calculated = Promise.promise();
        MemoryMapUnit mmu = MemoryMapUnit.shared();
        LongStream.range(0, partition).mapToObj((i) -> {
            long offset = i * slice;
            long size = Math.min(offset + slice, total) - offset;
            return mmu.map(file, offset, size);
        }).reduce((left, right) -> {
            return left.transformAsync((buffer) -> {
                crc.update(buffer);
                return right;
            }).addListener(() -> mmu.unmap(left.join()));
        }).ifPresent((promise) -> {
            promise.transform((buffer) -> {
                crc.update(buffer);
                calculated.complete(crc.getValue());
                return null;
            }).addListener(() -> mmu.unmap(promise.join()));
        });

        return calculated;
    }
}

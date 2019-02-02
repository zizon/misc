package com.sf.misc.antman.simple;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class TestMemoryMapUnit {

    static MemoryMapUnit mmu = MemoryMapUnit.shared();

    static File mmu_file = new File("__mmu__test__");

    @AfterClass
    public static void cleanup() {
        mmu_file.delete();
    }


    @Test
    public void test() {
        Assert.assertTrue(mmu.map(mmu_file, 0, 14)
                .transform((buffer) -> {
                    mmu.unmap(buffer);
                    return true;
                }).join()
        );
    }

}

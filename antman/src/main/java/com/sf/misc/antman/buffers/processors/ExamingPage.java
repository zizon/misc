package com.sf.misc.antman.buffers.processors;

import com.sf.misc.antman.Promise;
import com.sf.misc.antman.buffers.Page;
import com.sf.misc.antman.buffers.Page.AbstractPageProcessor;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class ExamingPage extends AbstractPageProcessor {

    @Override
    public boolean compatible(Page.PageType type) {
        return true;
    }

    @Override
    public byte type() {
        return 0b10;
    }

    @Override
    protected Promise<Void> touchPage(long page_id) {
        return this.mmap(page_id, FileChannel.MapMode.READ_ONLY).transform((buffer) -> {
            ByteBuffer newly = buffer.duplicate();
            ByteBuffer read = newly.duplicate();
            LOGGER.info("touch page:" + page_id + " in used?:" + read.get());
            return null;
        });
    }
}

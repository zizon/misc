package com.sf.misc.antman.buffers.processors;

import com.sf.misc.antman.Promise;
import com.sf.misc.antman.blocks.Block;
import com.sf.misc.antman.buffers.Page;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;

public class MutablePage extends Page.AbstractPageProcessor {

    @Override
    public byte type() {
        return 0b01;
    }

    @Override
    protected Promise<Void> touchPage(long page_id) {
        return Promise.success(null);
    }
}

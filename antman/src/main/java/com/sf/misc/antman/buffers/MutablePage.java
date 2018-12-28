package com.sf.misc.antman.buffers;

import com.sf.misc.antman.Promise;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;

public class MutablePage extends Page.AbstractPageProcessor {

    protected long my_offset;

    @Override
    protected Page initializeBuffer(MappedByteBuffer buffer) {
        // save orignal reference
        ByteBuffer mark = buffer.duplicate();

        return new Page(buffer) {
            @Override
            void reclaim() {
                // clean use mark
                ByteBuffer write = buffer.duplicate();
                write.position(0);

                // unmask
                write.put((byte) 0b0);
                this.sync();
            }
        };
    }

    @Override
    protected void touchPage(List<Long> offest) {
        // do nothing
    }

    @Override
    public byte type() {
        return 0b01;
    }
}

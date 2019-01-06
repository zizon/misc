package com.sf.misc.antman.simple.client;

import java.nio.ByteBuffer;
import java.util.UUID;

public class SteramChunk {

    protected final UUID stream_id;
    protected final long stream_offset;
    protected final ByteBuffer content;

    public SteramChunk(UUID stream_id, long stream_offset, ByteBuffer content) {
        this.stream_id = stream_id;
        this.stream_offset = stream_offset;
        this.content = content;
    }

    public UUID uuid() {
        return stream_id;
    }

    public long absoluteOffset() {
        return stream_offset;
    }


    public ByteBuffer content() {
        return content;
    }


}

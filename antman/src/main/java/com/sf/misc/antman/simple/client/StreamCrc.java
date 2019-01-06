package com.sf.misc.antman.simple.client;

import java.util.UUID;

public class StreamCrc {

    protected final UUID uuid;
    protected final long crc;
    protected final long size;

    public long getSize() {
        return size;
    }

    public UUID getUuid() {
        return uuid;
    }

    public long getCrc() {
        return crc;
    }

    public StreamCrc(UUID uuid, long crc, long size) {
        this.uuid = uuid;
        this.crc = crc;
        this.size = size;
    }
}

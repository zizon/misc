package com.sf.misc.antman.blocks;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Optional;
import java.util.UUID;

public class Block {

    public static final Log LOGGER = LogFactory.getLog(Block.class);

    protected static final byte VERSION = 0x01;

    protected UUID file_uuid;
    protected long from;
    protected long span;
    protected long size;
    protected Storages.BlockChannel channel;

    public static Optional<Block> mapped(UUID file_uuid, long from, long span, long size) {
        Storages.BlockChannel channel = Storages.lease();

        // initialized
        Block block = new Block(file_uuid, from, span, size, channel);
        try {
            block.init();
        } catch (IOException e) {
            LOGGER.error("fail to map block", e);
            Storages.reclaim(channel);
            return Optional.empty();
        }

        return Optional.of(block);
    }

    protected Block(UUID file_uuid, long from, long span, long size, Storages.BlockChannel channel) {
        this.file_uuid = file_uuid;
        this.from = from;
        this.span = span;
        this.size = size;
        this.channel = channel;
    }

    protected long metaSize() {
        return 1 // version
                + Long.BYTES * 2 // uuid
                + Long.BYTES // from seq
                + Long.BYTES // span seqs
                + Long.BYTES // size
                ;
    }

    protected Block init() throws IOException {
        // map meta
        MappedByteBuffer meta = null;
        try {
            meta = channel.channel().map(FileChannel.MapMode.READ_WRITE, 0, metaSize());
        } catch (IOException e) {
            throw new IOException("fail to map meta region", e);
        }

        // format
        meta.put(VERSION) // verions
                .putLong(file_uuid.getMostSignificantBits())
                .putLong(file_uuid.getLeastSignificantBits()) // file uuid
                .putLong(this.from) // from
                .putLong(this.span) // span
                .putLong(this.size) // size
        ;

        // persist
        meta.force();

        return this;
    }


}

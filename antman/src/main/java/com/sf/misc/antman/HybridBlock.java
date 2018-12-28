package com.sf.misc.antman;

import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.Map;
import java.util.UUID;
import java.util.zip.CRC32;

public class HybridBlock {

    public static final Log LOGGER = LogFactory.getLog(HybridBlock.class);

    protected final UUID block_uuid;
    protected final long seq;
    protected final int size;
    protected final int offest;
    protected final Map.Entry<UUID, MappedByteBuffer> buffer;
    protected final CRC32 crc;


    public HybridBlock(UUID block_uuid, long seq, int size) {
        this.block_uuid = block_uuid;
        this.seq = seq;
        this.size = size;
        this.crc = new CRC32();

        // calculate back memory requirement
        int total = 16 // uuid
                + Long.BYTES // seq
                + Integer.BYTES // block content size
                + Integer.BYTES // crc32
                + this.size // block size
                ;
        this.offest = 16 + Long.BYTES + Integer.BYTES;


        this.buffer = initializeMeta(HybridBlockPool.allocate(total));
    }


    public boolean write(ByteBuf buf) {
        int readable = buf.readableBytes();
        ByteBuffer local = this.buffer.getValue();

        // shadow copy,mark current position
        ByteBuffer write_buffer = local.duplicate();

        // fix write buffer
        if (write_buffer.remaining() > readable) {
            write_buffer.limit(write_buffer.position() + readable);
        }

        // read into
        buf.readBytes(write_buffer);

        // update crc
        // restore position
        ByteBuffer crc_buffer = write_buffer.duplicate();
        crc_buffer.flip();
        crc_buffer.position(local.position());
        crc.update(write_buffer);

        // update real buffer
        local.position(write_buffer.position());

        // end of block
        if (local.remaining() == Integer.BYTES) {
            // write crc
            local.putInt((int) crc.getValue());
            return true;
        } else if (local.remaining() < Integer.BYTES) {
            throw new IllegalStateException("block should finished with at least 4 bytes for crc,but remainds:" + local.remaining() + " local:" + local);
        }

        // not finised yet
        return false;
    }

    public void commit() {
        HybridBlockPool.finalized(block_uuid);
        this.buffer.setValue(null);
    }

    public void dispose() {
        HybridBlockPool.release(block_uuid);
        this.buffer.setValue(null);
    }

    public UUID uuid() {
        return this.block_uuid;
    }

    public int crc() {
        return (int) this.crc.getValue();
    }

    public long sequence() {
        return this.seq;
    }

    public String toString() {
        return "block:" + uuid() + " seq:" + sequence() + " size:" + this.size;
    }

    protected Map.Entry<UUID, MappedByteBuffer> initializeMeta(Map.Entry<UUID, MappedByteBuffer> entry) {
        MappedByteBuffer buffer = entry.getValue();

        // uuid
        buffer.putLong(this.block_uuid.getMostSignificantBits());
        buffer.putLong(this.block_uuid.getLeastSignificantBits());

        // seq
        buffer.putLong(this.seq);

        // block size
        buffer.putInt(this.size);

        // update crc
        ByteBuffer crc_buffer = buffer.duplicate();
        crc_buffer.flip();
        this.crc.update(crc_buffer);

        return entry;
    }


}

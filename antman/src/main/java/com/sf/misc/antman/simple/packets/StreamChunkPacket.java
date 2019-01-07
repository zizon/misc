package com.sf.misc.antman.simple.packets;

import com.sf.misc.antman.simple.Packet;
import com.sf.misc.antman.simple.server.ChunkServent;
import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.UUID;

public class StreamChunkPacket implements Packet.NoAckPacket {

    public static final Log LOGGER = LogFactory.getLog(StreamChunkPacket.class);

    protected UUID stream_id;
    protected long stream_offset;
    protected ByteBuffer content;

    protected StreamChunkPacket() {
    }

    public StreamChunkPacket(UUID stream_id, long stream_offset, ByteBuffer content) {
        this.stream_id = stream_id;
        this.stream_offset = stream_offset;
        this.content = content;
    }

    @Override
    public StreamChunkPacket decodePacket(ByteBuf from) {
        long header = Long.BYTES + Long.BYTES  // uuid
                + Long.BYTES // offset
                + Long.BYTES // stream length
                ;

        UUID stream_id = UUIDCodec.decode(from);
        long offset = from.readLong();
        long chunk_length = from.readLong();

        if (from.readableBytes() != chunk_length) {
            throw new RuntimeException("lenght not match,expect:" + chunk_length + " got:" + from.readableBytes() + " from:" + from + " uuid:" + stream_id + " offset:" + offset);
        }

        StreamChunkPacket chunk = new StreamChunkPacket(stream_id, offset, null);
        try {
            ByteBuffer source = ChunkServent.mmap(stream_id, offset, chunk_length);
            from.readBytes(source);

            return chunk;
        } catch (IOException e) {
            throw new UncheckedIOException("fail when decode stream chunk:" + this, e);
        }
    }

    @Override
    public void encodePacket(ByteBuf to) {
        // done
        UUIDCodec.encdoe(to, stream_id)// uuid
                .writeLong(stream_offset) // offfset
                .writeLong(content.remaining()) // length
                .writeBytes(content) // content
        ;
    }

    @Override
    public byte type() {
        return 0x01;
    }

    @Override
    public String toString() {
        return "Chunk:" + this.stream_id + " offset:" + this.stream_offset + " content:" + this.content;
    }
}

package com.sf.misc.antman.simple.packets;

import com.sf.misc.antman.Promise;
import com.sf.misc.antman.simple.server.ChunkServent;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.UUID;

public class StreamChunkPacket implements Packet {

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
    public void decodeComplete(ChannelHandlerContext ctx) {
        Promise.wrap(
                ctx.writeAndFlush(
                        new StreamChunkAckPacket(
                                stream_id,
                                stream_offset,
                                content.remaining())
                )
        ).addListener(() -> {
            ChunkServent.unmap(content).logException();
        }).catching(ctx::fireExceptionCaught);
    }

    @Override
    public void decodePacket(ByteBuf from) {
        long header = Long.BYTES + Long.BYTES  // uuid
                + Long.BYTES // offset
                + Long.BYTES // stream length
                ;

        this.stream_id = PacketCodec.decodeUUID(from);
        this.stream_offset = from.readLong();
        long chunk_length = from.readLong();

        if (from.readableBytes() != chunk_length) {
            throw new RuntimeException("lenght not match,expect:" + chunk_length + " got:" + from.readableBytes() + " from:" + from + " uuid:" + stream_id + " offset:" + stream_offset);
        }

        ByteBuffer source = ChunkServent.mmap(stream_id, stream_offset, chunk_length).join();
        from.readBytes(source.duplicate());

        this.content = source;
    }

    @Override
    public void encodePacket(ByteBuf to) {
        // done
        PacketCodec.encodeUUID(to, stream_id)// uuid
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

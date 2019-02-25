package com.sf.misc.antman.simple.packets;

import com.sf.misc.antman.Promise;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.nio.ByteBuffer;
import java.util.UUID;

public class StreamChunkPacket implements Packet.NoAckPacket {

    public static final Log LOGGER = LogFactory.getLog(StreamChunkPacket.class);

    @ProtocolField(order = 0)
    protected UUID stream_id;

    @ProtocolField(order = 1)
    protected long stream_offset;

    @ProtocolField(order = 2)
    protected ByteBuffer content;

    protected StreamChunkPacket() {
    }

    public StreamChunkPacket(UUID stream_id, long stream_offset, ByteBuffer content) {
        this.stream_id = stream_id;
        this.stream_offset = stream_offset;
        this.content = content;
    }

    @Override
    public void decodePacket(ByteBuf from) {
        throw new RuntimeException("client not supported");
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

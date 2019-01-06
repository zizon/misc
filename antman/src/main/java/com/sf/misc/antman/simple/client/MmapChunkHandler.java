package com.sf.misc.antman.simple.client;

import com.sf.misc.antman.simple.server.AntPacketInHandler;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.nio.ByteBuffer;

public class MmapChunkHandler extends MessageToByteEncoder<SteramChunk> {

    public static final Log LOGGER = LogFactory.getLog(MmapChunkHandler.class);

    public MmapChunkHandler() {
        super(true);
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, SteramChunk chunk, ByteBuf out) throws Exception {
        long header = Integer.BYTES // packet length
                + Byte.BYTES // version
                + Byte.BYTES // type
                + Long.BYTES + Long.BYTES  // uuid
                + Long.BYTES // offset
                + Long.BYTES // stream length
                ;

        int packet_legnth = Byte.BYTES // version
                + Byte.BYTES // type
                + Long.BYTES + Long.BYTES  // uuid
                + Long.BYTES // offset
                + Long.BYTES // stream length
                + chunk.content().capacity()  // content
                ;

        // done
        out.writeInt(packet_legnth); // packet length
        out.writeByte(AntPacketInHandler.VERSION); // version
        out.writeByte(AntPacketInHandler.STREAM_CHUNK_TYPE); // type
        out.writeLong(chunk.uuid().getMostSignificantBits()); // uuid
        out.writeLong(chunk.uuid().getLeastSignificantBits()); // uuid
        out.writeLong(chunk.absoluteOffset()); // offfset
        out.writeLong(chunk.content().capacity()); //
        out.writeBytes(chunk.content()); //

        return;
    }
}

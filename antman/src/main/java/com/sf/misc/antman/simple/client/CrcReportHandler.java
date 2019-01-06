package com.sf.misc.antman.simple.client;

import com.sf.misc.antman.simple.server.AntPacketInHandler;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.util.zip.CRC32;

public class CrcReportHandler extends MessageToByteEncoder<StreamCrc> {

    @Override
    protected void encode(ChannelHandlerContext ctx, StreamCrc msg, ByteBuf out) throws Exception {
        long header = Integer.BYTES // packet length
                + Byte.BYTES // version
                + Byte.BYTES // type
                + Long.BYTES + Long.BYTES  // uuid
                + Long.BYTES // crc
                + Long.BYTES // size
                ;

        int packet_length = +Byte.BYTES // version
                + Byte.BYTES // type
                + Long.BYTES + Long.BYTES  // uuid
                + Long.BYTES // crc
                + Long.BYTES // size
                ;

        out.writeInt(packet_length); // packet length
        out.writeByte(AntPacketInHandler.VERSION); // version
        out.writeByte(AntPacketInHandler.STREAM_CRC_TYPE); // type
        out.writeLong(msg.uuid.getMostSignificantBits()); // uuid
        out.writeLong(msg.uuid.getLeastSignificantBits()); // uuid
        out.writeLong(msg.crc); // crc
        out.writeLong(msg.size); // size
    }
}

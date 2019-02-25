package com.sf.misc.antman.simple.packets;

import com.sf.misc.antman.Promise;
import io.netty.channel.ChannelHandlerContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.zip.CRC32;

public class CommitStreamPacket implements Packet.NoAckPacket {

    public static final Log LOGGER = LogFactory.getLog(CommitStreamPacket.class);

    @ProtocolField(order = 0)
    protected UUID stream_id;

    @ProtocolField(order = 1)
    protected long length;

    @ProtocolField(order = 2)
    protected long crc;

    @ProtocolField(order = 3)
    protected boolean match;

    @ProtocolField(order = 4)
    protected UUID client_id;

    @ProtocolField(order = 5)
    protected String file_name;

    public CommitStreamPacket(UUID stream_id, long length, long crc, UUID client_id, String file_name) {
        this.stream_id = stream_id;
        this.length = length;
        this.crc = crc;
        this.client_id = client_id;
        this.file_name = file_name;
    }

    protected CommitStreamPacket() {
    }

    @Override
    public byte type() {
        return 0x02;
    }

    @Override
    public String toString() {
        return "Stream:" + stream_id + " crc:" + crc + " length:" + length;
    }
}

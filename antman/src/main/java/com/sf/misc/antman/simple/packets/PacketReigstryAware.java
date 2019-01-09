package com.sf.misc.antman.simple.packets;

import com.sf.misc.antman.simple.Packet;

public interface PacketReigstryAware {

    default Packet.Registry initializeRegistry(Packet.Registry registry) {
        return registry //
                .register(() -> new StreamChunkPacket()) //
                .register(() -> new CommitStreamPacket())
                .register(() -> new CommitStreamAckPacket())
                .repalce(()->new StreamChunkAckPacket())
                ;
    }
}

package com.sf.misc.antman.simple.packets;

public interface PacketReigstryAware {

    default Packet.Registry initializeRegistry(Packet.Registry registry) {
        return registry //
                .repalce(() -> new StreamChunkPacket()) //
                .repalce(() -> new CommitStreamPacket())
                .repalce(() -> new CommitStreamAckPacket())
                .repalce(()->new StreamChunkAckPacket())
                .repalce(()->new RequestCRCPacket())
                .repalce(()->new RequestCRCAckPacket())
                ;
    }
}

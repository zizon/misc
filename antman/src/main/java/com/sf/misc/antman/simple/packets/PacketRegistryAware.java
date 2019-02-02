package com.sf.misc.antman.simple.packets;

public interface PacketRegistryAware {

    default Packet.Registry initializeRegistry(Packet.Registry registry) {
        return registry //
                .repalce(() -> new StreamChunkPacket()) // 0x01
                .repalce(() -> new CommitStreamPacket()) // 0x02
                .repalce(() -> new CommitStreamAckPacket()) // 0x03
                .repalce(()->new StreamChunkAckPacket()) // 0x04
                .repalce(()->new RequestCRCPacket()) // 0x05
                .repalce(()->new RequestCRCAckPacket()) // 0x06
                ;
    }
}

package com.sf.misc.antman.simple.packets;

import com.sf.misc.antman.simple.Packet;
import com.sf.misc.antman.simple.packets.CrcAckPacket;
import com.sf.misc.antman.simple.packets.CrcReportPacket;
import com.sf.misc.antman.simple.packets.StreamChunkPacket;

public interface PacketReigstryAware {

    default Packet.Registry initializeRegistry(Packet.Registry registry) {
        return registry //
                .register(new StreamChunkPacket()) //
                .register(new CrcReportPacket())
                .register(new CrcAckPacket())
                ;
    }

    default Packet.Registry postInitializeRegistry(Packet.Registry registry) {
        return registry;
    }
}

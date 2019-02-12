package com.sf.misc.antman.simple.client;

import com.sf.misc.antman.Promise;
import com.sf.misc.antman.simple.packets.Packet;

import java.util.Optional;

public interface IOHandler extends AutoCloseable {

    Promise<?> connect();

    Promise<?> write(Packet packet);

    Optional<IOContext.Range> read();

    long readCRC();

    void close();
}

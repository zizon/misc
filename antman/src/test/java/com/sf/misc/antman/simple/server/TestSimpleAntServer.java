package com.sf.misc.antman.simple.server;

import com.sf.misc.antman.Promise;
import com.sf.misc.antman.TestClient;
import com.sf.misc.antman.simple.client.SimpleAntClient;
import com.sf.misc.antman.simple.server.SimpleAntServer;
import org.junit.Test;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.locks.LockSupport;

public class TestSimpleAntServer {

    @Test
    public void test() {
        File input_file = new File("large_exec_file.exe");
        SocketAddress address = new InetSocketAddress(10010);
        Promise<Void> serer_ready = Promise.wrap(new SimpleAntServer(address).bind()).logException();
        serer_ready.join();

        Promise<Void> uplaod = new SimpleAntClient(address).uploadFile(input_file).logException();
        uplaod.join();

        LockSupport.park();;
    }
}

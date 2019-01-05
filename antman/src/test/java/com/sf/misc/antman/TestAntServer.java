package com.sf.misc.antman;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.locks.LockSupport;

public class TestAntServer {

    public static final Log LOGGER = LogFactory.getLog(TestAntServer.class);

    @Test
    public void test() {
        File storage = new File("__storage__");
        storage.delete();
        File input_file = new File("large_exec_file.exe");

        SocketAddress address = new InetSocketAddress(10010);
        new AntServer(storage)
                .server(address)
                .sidekick(() -> LOGGER.info("bind"))
                .transformAsync((ignore) -> {
                    return new TestClient(address, input_file).start();
                })
                .logException();

        LockSupport.park();
    }
}

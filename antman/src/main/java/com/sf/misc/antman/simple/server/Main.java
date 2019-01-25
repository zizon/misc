package com.sf.misc.antman.simple.server;

import com.sf.misc.antman.Promise;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

public class Main {
    public static final Log LOGGER = LogFactory.getLog(Main.class);

    public static void main(String[] args) {
        String ip = args[0];
        int prot = Integer.parseInt(args[1]);

        SocketAddress address = new InetSocketAddress(ip, prot);
        LOGGER.info("try bind at:" + address);

        SimpleAntServer server = new SimpleAntServer(address);
        Promise<Void> serer_ready = Promise.wrap(server.bind()).logException();
        serer_ready.join();
        LOGGER.info("server bind,serving at:" + address);

        // wait for close
        Promise.wrap(server.closeFuture()).join();
    }
}

package com.sf.misc.antman.simple.server;

import com.sf.misc.antman.Promise;
import com.sf.misc.antman.simple.client.SimpleAntClient;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.locks.LockSupport;

public class TestSimpleAntServer {

    public static final Log LOGGER = LogFactory.getLog(TestSimpleAntServer.class);

    static File large_file = new File("__large_file__");

    @BeforeClass
    public static void setup() throws IOException {
        byte[] direct = new byte[1024 * 1024 * 10];
        long unit = direct.length;
        Random random = new Random();

        long generate_file = random.nextInt(10) * 100 * 1024 * 1024 // mb
                + random.nextInt(10) * 1024 //  kb
                + random.nextInt(10) // byte
                + 1;
        LOGGER.info("generate large file size:" + generate_file);

        long runs = generate_file / unit +
                ((generate_file % unit) != 0 ? 1 : 0);
        try (FileChannel channel = FileChannel.open(large_file.toPath(), StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE)) {
            for (long offset = 0; offset < generate_file; offset += unit) {
                int length = (int) Math.min(unit, generate_file - offset);
                random.nextBytes(direct);
                channel.map(FileChannel.MapMode.READ_WRITE, offset, length)
                        .put(direct, 0, length);
            }
        }
    }

    @AfterClass
    public static void teraup() {
        large_file.delete();
    }

    @Test
    public void test() {
        File input_file = large_file;
        UUID stream_id = UUID.nameUUIDFromBytes(input_file.toURI().toString().getBytes());

        LOGGER.info(input_file.length());
        SocketAddress address = new InetSocketAddress(10010);
        Promise<SimpleAntServer> serer_ready = new SimpleAntServer(address).bind();
        serer_ready.join();

        Promise<Boolean> match = serer_ready.transformAsync((ignore) ->
                new SimpleAntClient(address)
                        .uploadFile(stream_id, input_file, (size) -> {
                            LOGGER.info("ack:" + size);
                        })
        );

        // join
        Promise.all(serer_ready, match).transform((ignore) -> {
            Assert.assertTrue(match.join());
            return null;
        }).logException().join();
    }
}

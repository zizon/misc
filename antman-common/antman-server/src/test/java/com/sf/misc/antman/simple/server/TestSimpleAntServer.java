package com.sf.misc.antman.simple.server;

import com.sf.misc.antman.Promise;
import com.sf.misc.antman.simple.ChunkServent;
import com.sf.misc.antman.simple.MemoryMapUnit;
import com.sf.misc.antman.simple.client.Client;
import com.sf.misc.antman.simple.client.TuningParameters;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.UUID;

public class TestSimpleAntServer {

    public static final Log LOGGER = LogFactory.getLog(TestSimpleAntServer.class);

    static File large_file = new File("__large_file__");
    static File small_file = new File("__smaell_file__");

    @BeforeClass
    public static void setup() throws IOException {
        Promise.PromiseConsumer<Path> delete = new Promise.PromiseConsumer<Path>() {
            @Override
            public void internalAccept(Path path) throws Throwable {
                if (Files.isDirectory(path)) {
                    Files.list(path).forEach(this::accept);
                } else {
                    Files.delete(path);
                }
            }
        };

        delete.accept(ChunkServent.STORAGE.toPath());

        byte[] direct = new byte[1024 * 1024 * 10];
        long unit = direct.length;
        Random random = new Random();

        long generate_file = (random.nextInt(9) + 1) * 10 * 1024 * 1024L // mb
                + random.nextInt(10) * 1024L //  kb
                + random.nextInt(10) * 1L // byte
                + 1L;
        //generate_file = 1024 * 1024 * 16;
        LOGGER.info("generate large file size:" + generate_file);

        try (RandomAccessFile truncate = new RandomAccessFile(large_file, "rw")) {
            truncate.setLength(generate_file);
        }


        for (long offset = 0; offset < generate_file; offset += unit) {
            int length = (int) Math.min(unit, generate_file - offset);
            random.nextBytes(direct);
            ByteBuffer buffer = MemoryMapUnit.shared().map(large_file, offset, length).join();
            buffer.put(direct, 0, length);
            MemoryMapUnit.shared().unmap(buffer);
        }
        LOGGER.info("generate file ok");

        try (OutputStream output = new FileOutputStream(small_file)) {
            output.write("hello kitty".getBytes());
        }
    }

    @AfterClass
    public static void teraup() {
        large_file.delete();
        small_file.delete();
    }

    @Test
    public void testClient() {
        File input_file = large_file;
        UUID stream_id = UUID.nameUUIDFromBytes(input_file.toURI().toString().getBytes());

        LOGGER.info("file length:" + input_file.length());
        SocketAddress address = new InetSocketAddress(10010);
        Promise<SimpleAntServer> serer_ready = SimpleAntServer.create(address);

        Client client = new Client(address);
        Client.Session session = client.upload(input_file, stream_id, new Client.SessionListener() {
            @Override
            public void onRangeFail(long offset, long size, Throwable reason) {
                LOGGER.error("range fail:" + offset + " size:" + size, reason);
            }

            @Override
            public void onUploadSuccess() {
                LOGGER.info("upload ok");
            }

            @Override
            public void onChannelComplete() {
                LOGGER.info("channel closed");
            }

            @Override
            public void onProgress(long commited, long failed, long going, long expected) {
                LOGGER.info("commited:" + commited + " failed:" + failed + " going:" + going + " expected:" + expected);
            }
        }, TuningParameters.DEFAULT);

        try {
            session.tryUpload().get();
            LOGGER.info("first done");
        } catch (Throwable throwable) {
            LOGGER.error("fail client test", throwable);
            //Assert.fail();
        }

        if (!session.isAllCommit()) {
            LOGGER.info("sencond try...");
            Promise.wrap(session.tryUpload()).join();
        }
        Assert.assertTrue(session.isAllCommit());

    }

}

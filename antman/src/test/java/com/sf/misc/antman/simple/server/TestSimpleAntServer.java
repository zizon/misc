package com.sf.misc.antman.simple.server;

import com.sf.misc.antman.Promise;
import com.sf.misc.antman.simple.MemoryMapUnit;
import com.sf.misc.antman.simple.client.SimpleAntClient;
import com.sf.misc.antman.simple.client.UploadSession;
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
import java.nio.channels.FileChannel;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.zip.CRC32;

public class TestSimpleAntServer {

    public static final Log LOGGER = LogFactory.getLog(TestSimpleAntServer.class);

    static File large_file = new File("__large_file__");
    static File small_file = new File("__smaell_file__");

    @BeforeClass
    public static void setup() throws IOException {
        byte[] direct = new byte[1024 * 1024 * 10];
        long unit = direct.length;
        Random random = new Random();

        long generate_file = (random.nextInt(9) + 1) * 10 * 1024 * 1024 // mb
                + random.nextInt(10) * 1024 //  kb
                + random.nextInt(10) // byte
                + 1;
        //generate_file = 4 * 1024+2;
        LOGGER.info("generate large file size:" + generate_file);

        try (RandomAccessFile truncate = new RandomAccessFile(large_file, "rw")) {
            truncate.setLength(generate_file);
        }

        try (FileChannel channel = FileChannel.open(large_file.toPath(), StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE)) {
            for (long offset = 0; offset < generate_file; offset += unit) {
                int length = (int) Math.min(unit, generate_file - offset);
                random.nextBytes(direct);
                channel.map(FileChannel.MapMode.READ_WRITE, offset, length)
                        .put(direct, 0, length);
            }
        }

        try (OutputStream output = new FileOutputStream(small_file)) {
            output.write("hello kitty".getBytes());
        }
    }

    @AfterClass
    public static void teraup() {
        //large_file.delete();
        small_file.delete();

        Promise.PromiseFunction<File, Stream<File>> list = (file) -> Arrays.stream(file.listFiles());

        /*
        list.apply(ChunkServent.STORAGE).parallel()
                .flatMap((child) -> {
                    if (child.isFile()) {
                        return Stream.of(child);
                    }

                    return list.apply(child);
                })
                .forEach(File::delete);
    */
    }

    @Test
    public void testOldClient() {
        File input_file = large_file;
        UUID stream_id = UUID.nameUUIDFromBytes(input_file.toURI().toString().getBytes());

        LOGGER.info("file length:" + input_file.length());
        SocketAddress address = new InetSocketAddress(10010);
        Promise<SimpleAntServer> serer_ready = SimpleAntServer.create(address);

        Promise<Boolean> match = serer_ready.transformAsync((ignore) ->
                new com.sf.misc.antman.simple.client.deprecated.SimpleAntClient(address)
                        .uploadFile(stream_id, input_file, (size) -> {
                            LOGGER.info("ack:" + size);
                        })
        );

        // join
        Promise.all(serer_ready, match).transform((ignore) -> {
            Assert.assertTrue(match.join());
            SimpleAntServer server = serer_ready.join();

            // clean up
            server.close();
            server.onClose().join();
            return null;
        }).logException().join();
    }

    @Test
    public void testNewClient() {
        File input_file = large_file;
        UUID stream_id = UUID.nameUUIDFromBytes(input_file.toURI().toString().getBytes());

        LOGGER.info("file length:" + input_file.length());
        SocketAddress address = new InetSocketAddress(10010);
        Promise<SimpleAntServer> serer_ready = SimpleAntServer.create(address);

        Promise<SimpleAntClient> client_ready = SimpleAntClient.create(address);

        // upload
        Promise<SimpleAntClient.ChannelSession> session_ready = Promise.all(serer_ready, client_ready).transform((ignore) -> {
            SimpleAntClient client = client_ready.join();
            return client.upload(stream_id, input_file);
        });
        // completeion
        Promise<?> complection = session_ready.transformAsync((session) -> session.completion());

        // setup state litner
        session_ready.sidekick((session) -> {
            session.addStateListener(new UploadSession.StateListener() {
                long progress = 0;

                @Override
                public void onProgress(long acked, long expected) {
                    long now = (long) Math.floor(((double) acked / expected) * 100);
                    if (now > progress) {
                        LOGGER.info("upload stream:" + stream_id + " progress:(" + (acked + "/" + expected) + ") :" + progress + "%");
                        progress = now;
                    }
                }

                @Override
                public void onSuccess(long effected_time) {
                    LOGGER.info("uploaded,cost:" + effected_time);
                }

                @Override
                public void onTimeout(UploadSession.Range range, long expire) {
                    //LOGGER.warn("stream:" + stream_id + " range:" + range + " timeout:" + expire);
                }

                @Override
                public void onUnRecovable(Throwable reason) {
                    LOGGER.error("unrecovable transmission for stream:" + stream_id, reason);
                    Assert.fail();
                }

                @Override
                public boolean onFail(UploadSession.Range range, Throwable cause) {
                    //LOGGER.warn("stream:" + stream_id + " fail range:" + range + " retry...", cause);
                    return true;
                }
            });
        });

        complection.join();
        Assert.assertFalse(complection.isCompletedExceptionally());

        MemoryMapUnit.shared().map(large_file, 0, large_file.length()).transform((buffer) -> {
            CRC32 crc = new CRC32();
            crc.update(buffer);
            LOGGER.info("crc:" + crc.getValue());

            return MemoryMapUnit.shared().unmap(buffer);
        }).join();


    }
}

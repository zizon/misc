package com.sf.misc.antman.simple.server;

import com.sf.misc.antman.Promise;
import com.sf.misc.antman.simple.MemoryMapUnit;
import com.sf.misc.antman.simple.client.ClientIO;
import com.sf.misc.antman.simple.client.SimpleAntClient;
import com.sf.misc.antman.simple.client.TuningParameters;
import com.sf.misc.antman.simple.client.UploadChannel;
import com.sf.misc.antman.simple.client.UploadSession;
import com.sf.misc.antman.simple.packets.Packet;
import io.netty.channel.Channel;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Log4J2LoggerFactory;
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
import java.nio.channels.FileChannel;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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

        Promise.PromiseFunction<File, Stream<File>> list = (file) -> Arrays.stream(file.listFiles());


        list.apply(ChunkServent.STORAGE).parallel()
                .flatMap((child) -> {
                    if (child.isFile()) {
                        return Stream.of(child);
                    }

                    return list.apply(child);
                })
                .forEach(File::delete);
    }

    @Test
    public void testNewClient() {
        File input_file = large_file;
        UUID stream_id = UUID.nameUUIDFromBytes(input_file.toURI().toString().getBytes());

        LOGGER.info("file length:" + input_file.length());
        SocketAddress address = new InetSocketAddress(10010);
        Promise<SimpleAntServer> serer_ready = SimpleAntServer.create(address);

        Promise<SimpleAntClient> client_ready = SimpleAntClient.create(address, new TuningParameters() {
            @Override
            public long scheduleCostPerChunk() {
                return 1;
            }
        });

        // upload
        Promise<SimpleAntClient.ChannelSession> session_ready = Promise.all(client_ready).transform((ignore) -> {
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
                    LOGGER.info("upload stream:" + stream_id + " progress:(" + (acked + "/" + expected) + ") :" + progress + "%");
                }

                @Override
                public void onSuccess(long effected_time) {
                    LOGGER.info("uploaded,cost:" + effected_time);
                }

                @Override
                public void onTimeout(UploadSession.Range range, long expire) {
                    LOGGER.warn("stream:" + stream_id + " range:" + range + " timeout:" + expire);
                }

                @Override
                public void onUnRecovable(Throwable reason) {
                    LOGGER.error("unrecovable transmission for stream:" + stream_id, reason);
                    Assert.fail();
                }

                @Override
                public boolean onFail(UploadSession.Range range, Throwable cause) {
                    LOGGER.warn("stream:" + stream_id + " fail range:" + range + " retry...", cause);
                    Assert.fail();
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

    @Test
    public void testClientIO() {
        File input_file = large_file;
        UUID stream_id = UUID.nameUUIDFromBytes(input_file.toURI().toString().getBytes());

        LOGGER.info("file length:" + input_file.length());
        SocketAddress address = new InetSocketAddress(10010);
        Promise<SimpleAntServer> serer_ready = SimpleAntServer.create(address);
        serer_ready.join();
        Promise.delay(() -> {
            serer_ready.join().close();
        }, 2000);

        Promise<?> done = Promise.promise();
        UploadChannel client = new ClientIO().connect(address);
        client.upload(input_file, stream_id, new UploadChannel.ProgressListener() {
            @Override
            public void onProgress(long commited, long failed, long going, long total) {
                LOGGER.info("progress, commited:" + commited + " failed:" + failed + " going:" + going + " total:" + total);
            }
        }, new UploadChannel.FailureListener() {
            @Override
            public void onUploadFail(File file, UUID stream_id, Throwable reason) {
                LOGGER.error("fail to uplaod:" + file, reason);
                done.completeExceptionally(reason);
            }
        }, new UploadChannel.RetryPolicy() {
            @Override
            public boolean shouldRetry(long offset, long size, int had_retrid, Throwable fail_reason) {
                return false;
            }
        }, new UploadChannel.SuccessListener() {
            @Override
            public void onSuccess(File file, UUID stream_id) {
                Assert.assertTrue(true);
                done.complete(null);
            }
        });

        done.join();
        Assert.assertFalse(done.isCompletedExceptionally());
    }

}

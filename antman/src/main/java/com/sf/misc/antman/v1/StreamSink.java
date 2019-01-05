package com.sf.misc.antman.v1;

import com.sf.misc.antman.io.ByteStream;
import com.sf.misc.antman.ClosableAware;
import com.sf.misc.antman.Promise;
import com.sf.misc.antman.v1.processors.StreamCrcAckProcessor;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;

public class StreamSink {

    public static final Log LOGGER = LogFactory.getLog(StreamSink.class);

    protected static final StreamCrcAckProcessor CRC_ACK = new StreamCrcAckProcessor();
    protected static final File BUCKET_ROOT = new File("buckets");

    public static Promise<ByteBuf> flush(ByteStream.StreamContext context, long crc, long lenth, ByteBufAllocator allocator) {
        return requestStorageLocation().transform((location) -> new File(location, context.uuid().toString()))
                .transformAsync((file) -> {
                    // ensure length
                    return ClosableAware.wrap(() -> new RandomAccessFile(file, "rw")).transform((readwrite) -> {
                        readwrite.setLength(lenth);
                        return FileChannel.open(
                                file.toPath(),
                                StandardOpenOption.READ,
                                StandardOpenOption.WRITE,
                                StandardOpenOption.CREATE
                        );
                    }).transformAsync((channel) -> {
                        MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, lenth);
                        ByteBuffer writeout = buffer.duplicate();
                        context.tranferTo(writeout);
                        buffer.force();

                        CRC32 recheck = new CRC32();
                        recheck.update(buffer);

                        long final_crc = recheck.getValue();
                        if (final_crc != crc) {
                            LOGGER.warn("final output crc not match for context:" + context + " expected:" + crc + " got:" + final_crc);
                            Promise.light(() -> file.delete()).logException();
                            return badCRC(context, allocator);
                        }

                        // free stream resoruces
                        Promise.light(() -> StreamContextLookup.destroy(context.uuid()))
                                .logException();
                        return fineCRC(context, allocator);
                    });
                });
    }

    public static Promise<ByteBuf> badCRC(ByteStream.StreamContext context, ByteBufAllocator allocator) {
        return CRC_ACK.ackCRC(context, true, allocator);
    }

    public static Promise<ByteBuf> fineCRC(ByteStream.StreamContext context, ByteBufAllocator allocator) {
        return CRC_ACK.ackCRC(context, false, allocator);
    }

    protected static Promise<File> requestStorageLocation() {
        File bucket = new File(BUCKET_ROOT, "bucket-" + System.currentTimeMillis() % 1000);
        return Promise.light(() -> bucket.mkdirs()).transform((ignore) -> bucket);
    }
}

package com.sf.misc.antman;

import com.sf.misc.antman.v1.AntServerHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.file.StandardOpenOption;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.zip.CRC32;

public class TestClient {

    public static final Log LOGGER = LogFactory.getLog(TestClient.class);

    protected static class Context {
        protected ByteBuffer read_buffer;
        protected MappedByteBuffer content;
        protected ByteBuffer write_buffer;
        protected boolean write_crc;
        protected UUID uuid = new UUID(1024, 1024);

        public Context(MappedByteBuffer content) {
            this.content = content;
            this.read_buffer = ByteBuffer.allocateDirect(1024 * 1024);
            this.write_buffer = ByteBuffer.allocateDirect(1024 * 1024);
            write_buffer.limit(0);
            write_crc = false;
        }
    }

    protected final File test_file;
    protected final Promise<Void> start;

    public TestClient(SocketAddress server, File test_file) {
        this.test_file = test_file;
        this.start = Promise.costly(() -> {
            SocketChannel channel = SocketChannel.open(server);
            channel.configureBlocking(false);

            LOGGER.info("open selector");
            Selector selector = Selector.open();
            // register
            synchronized (selector.wakeup()) {
                SelectionKey connect_key = channel.register(selector, SelectionKey.OP_CONNECT | SelectionKey.OP_READ | SelectionKey.OP_WRITE);
            }

            Promise.light(() -> channel.connect(server))
                    .costly(() -> doIO(selector))
                    .catching((throwable) -> {
                        Stream.of(channel, selector).forEach((value) -> ClosableAware.wrap(() -> value).execute((instance) -> {
                            LOGGER.info("close:" + instance);
                        }));
                    }).logException();

            return channel;
        }).transform((channel) -> null);
    }

    public Promise<Void> start() {
        return start;
    }

    protected void doIO(Selector selector) throws Throwable {

        for (; ; ) {
            synchronized (selector) {
                if (selector.select() > 0) {
                    ;
                }
            }

            for (SelectionKey key : selector.selectedKeys()) {
                int ops = key.interestOps();

                // handel connect
                if ((ops & SelectionKey.OP_CONNECT) > 0) {
                    LOGGER.info("channel:" + key.channel() + " conencted");
                    key.interestOps(ops ^ SelectionKey.OP_CONNECT);

                    // attatch buffer
                    key.attach(new Context(
                                    ClosableAware.wrap(() -> FileChannel.open(test_file.toPath(), StandardOpenOption.READ))
                                            .transform((channel) -> {
                                                return channel.map(FileChannel.MapMode.READ_ONLY, 0, test_file.length());
                                            }).join()
                            )
                    );
                }

                //
                if ((ops & SelectionKey.OP_READ) > 0) {
                    // do read
                    doRead(key);
                }

                if ((ops & SelectionKey.OP_WRITE) > 0) {
                    // do write
                    doWrite(key);
                }

                key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
            }

        }
    }

    protected void doRead(SelectionKey key) throws Throwable {
        Context context = Context.class.cast(key.attachment());
        SocketChannel channel = (SocketChannel) key.channel();
        ByteBuffer local = context.read_buffer;

        // is full?
        for (; ; ) {
            if (!local.hasRemaining()) {
                // resize
                ByteBuffer new_buffer = ByteBuffer.allocateDirect(local.capacity() + 1024 * 1024);
                ByteBuffer copy = local.duplicate();
                copy.flip();
                new_buffer.put(copy);
                local = new_buffer;
            }

            int readed = channel.read(local);
            if (readed == -1) {
                throw new RuntimeException("read eof");
            } else if (readed == 0) {
                // no more in channel
                break;
            }
        }


        // process packet
        for (; ; ) {
            // to read mode
            ByteBuffer examin = local.duplicate();
            examin.flip();

            // unpack
            if (examin.remaining() < Integer.BYTES) {
                break;
            }
            int length = examin.getInt();
            if (examin.remaining() < length) {
                // not complete packet
                break;
            }

            // version
            byte version = examin.get();
            if (version != AntServerHandler.VERSION) {
                throw new RuntimeException("verserion not match");
            }

            // type
            byte type = examin.get();

            switch (type) {
                case 0x01:
                case 0x02:
                default:
                    throw new RuntimeException("not supportd packet:" + type);
                case 0x03:
                    // crc ack
                    long high = examin.getLong();
                    long low = examin.getLong();
                    boolean ok = examin.get() == 0x01;

                    LOGGER.info("stream:" + new UUID(high, low) + " crc ok:" + ok);
                    channel.close();
                    break;
            }

            examin.compact();
            local = examin;
        }

        context.read_buffer = local;
    }

    protected void doWrite(SelectionKey key) throws Throwable {
        Context context = Context.class.cast(key.attachment());
        SocketChannel channel = (SocketChannel) key.channel();
        ByteBuffer local = context.write_buffer;
        ByteBuffer content = context.content;

        // special case
        if (!local.hasRemaining()) {
            // new batch

            // make a write buffer
            long basic_unit = 128; // 128;
            long max_unit = 100;
            long selected_unit = System.currentTimeMillis() % max_unit;
            long unit_size = basic_unit * selected_unit;

            // then some randome number
            long random_unit = 10;
            selected_unit = System.currentTimeMillis() % max_unit;
            long tail_size = random_unit * selected_unit;

            // expected
            long expected_total_bytes = unit_size + tail_size;
            if (content.remaining() < expected_total_bytes) {
                expected_total_bytes = content.remaining();
            }

            // slice
            ByteBuffer slice = content.duplicate();
            slice.limit((int) (slice.position() + expected_total_bytes));
            slice = slice.slice();

            // build up packet
            // check if can build packet
            int header = Integer.BYTES// packet length
                    + Byte.BYTES // version
                    + Byte.BYTES // packet type
                    + Long.BYTES + Long.BYTES  // uuid
                    + Long.BYTES // offset
                    + Long.BYTES // stream length
                    ;
            int need_size = header + slice.remaining();

            // minus packet lenght field
            int packet_length = need_size - Integer.BYTES;
            long content_length = slice.remaining();

            // if buffser large enough
            if (local.capacity() < need_size) {
                local = ByteBuffer.allocateDirect(need_size);
            }
            local.position(0);
            local.limit(need_size);

            LOGGER.debug("send stream chunk:" + context.uuid + " offset:" + content.position() + " legnth:" + content_length);

            // build packet
            local.putInt(packet_length); // length
            local.put(AntServerHandler.VERSION); // version
            local.put((byte) 0x01); // stream chunk
            local.putLong(context.uuid.getMostSignificantBits()); // uuid high
            local.putLong(context.uuid.getLeastSignificantBits()); // uuid low
            local.putLong(content.position()); // offset
            local.putLong(content_length); // content length
            local.put(slice);

            local.flip();

            // move content
            content.position((int) (content.position() + content_length));
        }

        if (!local.hasRemaining() && !context.write_crc) {
            // make crc
            // build up packet
            // check if can build packet
            int header = Integer.BYTES// packet length
                    + Byte.BYTES // version
                    + Byte.BYTES // packet type
                    + Long.BYTES + Long.BYTES  // uuid
                    + Long.BYTES // length
                    + Long.BYTES // crc
                    ;

            int need_size = header;
            if (local.capacity() < need_size) {
                local = ByteBuffer.allocateDirect(need_size);
            }

            int packet_size = need_size - Integer.BYTES;

            // build packet
            local.putInt(packet_size); // length
            local.put(AntServerHandler.VERSION); // version
            local.put((byte) 0x02); // report
            local.putLong(context.uuid.getMostSignificantBits()); // uuid high
            local.putLong(context.uuid.getLeastSignificantBits()); // uuid low
            local.putLong(content.capacity());

            ByteBuffer copy = content.duplicate();
            content.reset();
            CRC32 crc = new CRC32();
            crc.update(copy);
            LOGGER.info("calculate stream:" + context.uuid + " crc:" + crc.getValue());
            local.putLong(crc.getValue());

            local.flip();
            context.write_crc = true;
        }

        // continue write
        for (; ; ) {
            // then write packet
            int wrote = channel.write(local);
            if (wrote == -1) {
                throw new RuntimeException("write eof");
            } else if (wrote == 0) {
                // can not write more
                break;
            }
        }

        // set back
        context.write_buffer = local;
    }
}

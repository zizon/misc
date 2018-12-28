package com.sf.misc.antman.buffers;

import com.sf.misc.antman.ClosableAware;
import com.sf.misc.antman.Promise;
import sun.misc.Cleaner;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.TimeUnit;

public class ResizableFileChannel extends FileChannel {

    protected FileChannel delegate;
    protected final File reference;

    public ResizableFileChannel(File file) throws IOException {
        this.reference = file;
        this.open();
    }

    public File file() {
        return this.reference;
    }

    public Promise<Void> setLength(long size) {
        return ClosableAware.wrap(() -> new RandomAccessFile(reference, "rw")).execute((file) -> {
            file.setLength(size);
        }).sidekick(() -> {
            open();
        }).transform((ignore) -> null);
    }

    protected void open() throws IOException {
        FileChannel old = this.delegate;
        this.delegate = FileChannel.open(
                this.reference.toPath(),
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.SYNC
        );

        if (old != null && this.delegate != old) {
            // close in 1 minutes later
            Promise.delay(() -> old.close(), TimeUnit.MINUTES.toMillis(1)).logException();
        }
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        return this.delegate.read(dst);
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        return this.delegate.read(dsts, offset, length);
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        return this.delegate.write(src);
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        return this.delegate.write(srcs, offset, length);
    }

    @Override
    public long position() throws IOException {
        return this.delegate.position();
    }

    @Override
    public FileChannel position(long newPosition) throws IOException {
        return this.delegate.position(newPosition);
    }

    @Override
    public long size() throws IOException {
        return this.delegate.size();
    }

    @Override
    public FileChannel truncate(long size) throws IOException {
        return this.delegate.truncate(size);
    }

    @Override
    public void force(boolean metaData) throws IOException {
        this.delegate.force(metaData);
    }

    @Override
    public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
        return this.delegate.transferTo(position, count, target);
    }

    @Override
    public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
        return this.delegate.transferFrom(src, position, count);
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        return this.delegate.read(dst, position);
    }

    @Override
    public int write(ByteBuffer src, long position) throws IOException {
        return this.delegate.write(src, position);
    }

    @Override
    public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
        return this.delegate.map(mode, position, size);
    }

    @Override
    public FileLock lock(long position, long size, boolean shared) throws IOException {
        return this.delegate.lock(position, size, shared);
    }

    @Override
    public FileLock tryLock(long position, long size, boolean shared) throws IOException {
        return this.delegate.lock(position, size, shared);
    }

    @Override
    protected void implCloseChannel() throws IOException {
        this.delegate.close();
    }
}

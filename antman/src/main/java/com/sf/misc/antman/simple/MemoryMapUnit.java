package com.sf.misc.antman.simple;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.sf.misc.antman.Promise;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import sun.misc.Cleaner;

import java.io.File;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.time.Duration;

public interface MemoryMapUnit {

    public static final Log LOGGER = LogFactory.getLog(MemoryMapUnit.class);

    static Promise.PromiseFunction<MappedByteBuffer, Cleaner> DEFUALT_CLEANER = new Promise.PromiseFunction<MappedByteBuffer, Cleaner>() {
        final MethodHandle cleaner;

        {
            try {
                MethodHandles.Lookup lookup = MethodHandles.publicLookup();

                // cleaner field
                Class<?> direct = Class.forName("java.nio.DirectByteBuffer");
                Field field = direct.getDeclaredField("cleaner");
                field.setAccessible(true);
                MethodHandle direct_clean = lookup.unreflectGetter(field);

                cleaner = MethodHandles.explicitCastArguments(direct_clean, MethodType.methodType(
                        Cleaner.class,
                        MappedByteBuffer.class
                ));
            } catch (Throwable e) {
                throw new RuntimeException("no direct buffer found", e);
            }

        }

        @Override
        public Cleaner internalApply(MappedByteBuffer buffer) throws Throwable {
            return (Cleaner) cleaner.invokeExact(buffer);
        }
    };

    static LoadingCache<File, Promise<FileChannel>> CHANNELS = CacheBuilder.newBuilder()
            .expireAfterAccess(Duration.ofMinutes(1))
            .softValues()
            .removalListener(new RemovalListener<File, Promise<FileChannel>>() {
                @Override
                public void onRemoval(RemovalNotification<File, Promise<FileChannel>> notification) {
                    FileChannel channel = notification.getValue().join();
                    Promise.PromiseRunnable close = channel::close;
                    Cleaner.create(channel, close);
                }
            }).build(new CacheLoader<File, Promise<FileChannel>>() {
                @Override
                public Promise<FileChannel> load(File key) throws Exception {
                    return Promise.light(
                            () -> FileChannel.open(
                                    key.toPath(),
                                    StandardOpenOption.READ,
                                    StandardOpenOption.WRITE,
                                    StandardOpenOption.CREATE
                            )
                    );
                }
            });

    static MemoryMapUnit SHARE_MMU = new MemoryMapUnit() {
    };

    static MemoryMapUnit shared() {
        return SHARE_MMU;
    }

    default Promise<FileChannel> channel(File file) {
        return CHANNELS.getUnchecked(file);
    }

    default Cleaner cleaner(MappedByteBuffer buffer) {
        return DEFUALT_CLEANER.apply(buffer);
    }

    default Promise<ByteBuffer> map(File file, long offset, long size) {
        return channel(file).transform((channel) -> {
            return channel.map(FileChannel.MapMode.READ_WRITE, offset, size);
        });
    }

    default Promise<?> unmap(ByteBuffer buffer) {
        if (buffer instanceof MappedByteBuffer) {
            MappedByteBuffer mapped = (MappedByteBuffer) buffer;
            try {
                cleaner(mapped).clean();
            } catch (Throwable throwable) {
                return Promise.exceptional(() -> throwable);
            }
        }

        return Promise.success(null);
    }
}

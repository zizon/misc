package com.sf.misc.antman.simple;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.sf.misc.antman.Cleaner;
import com.sf.misc.antman.LightReflect;
import com.sf.misc.antman.Promise;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;

public interface MemoryMapUnit {

    Log LOGGER = LogFactory.getLog(MemoryMapUnit.class);

    MethodHandle CLEAN_DIRECT_BUFFER = createCleanDirectByteBufferHandler();

    static MethodHandle createCleanDirectByteBufferHandler() {
        LightReflect reflect = LightReflect.share();
        List<Promise.PromiseRunnable> messages = new LinkedList<>();

        // jdk 9 case
        try {
            Class<?> jdk_9_unsafe = Class.forName("sun.misc.Unsafe");
            MethodHandle invoke_cleaner = reflect.method(
                    jdk_9_unsafe,
                    "invokeCleaner",
                    MethodType.methodType(
                            void.class,
                            ByteBuffer.class
                    )
            ).orElseThrow(() -> new NoSuchMethodException("no invokeCleaner of sun.misc.Unsafe"));

            // get unsafe
            Field unsafe_field = jdk_9_unsafe.getDeclaredField("theUnsafe");
            unsafe_field.setAccessible(true);

            // bind unsafe
            return reflect.invokable(invoke_cleaner.bindTo(unsafe_field.get(null)));
        } catch (ClassNotFoundException | NoSuchMethodException | NoSuchFieldException | IllegalAccessException e) {
            messages.add(() -> LOGGER.warn("fail to find jdk9 clean direct buffer handler", e));
        }

        // jdk8 case
        try {
            Class<?> direct_byte_buffer = Class.forName("java.nio.DirectByteBuffer");
            Class<?> cleaner = Class.forName("sun.misc.Cleaner");

            MethodHandle cleaner_handler = reflect.method(
                    direct_byte_buffer,
                    "cleaner",
                    MethodType.methodType(cleaner)
            ).orElseThrow(() -> new NoSuchMethodException("no cleanr method for:" + direct_byte_buffer));

            MethodHandle clean_handler = reflect.method(
                    cleaner,
                    "clean",
                    MethodType.methodType(void.class)
            ).orElseThrow(() -> new NoSuchMethodException("no clean method for:" + cleaner));

            MethodHandle prototype = MethodHandles.exactInvoker(MethodType.methodType(void.class, ByteBuffer.class));
            MethodHandle return_cleaner = MethodHandles.filterReturnValue(prototype, cleaner_handler);
            MethodHandle cleaner_clean = MethodHandles.filterReturnValue(return_cleaner, clean_handler);

            return reflect.invokable(cleaner_clean);
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            LOGGER.warn("fail to find jdk8 clean direct buffer handler", e);
        }

        // message out
        messages.forEach(Runnable::run);
        throw new RuntimeException("fail to create direct byte buffer cleaner");
    }

    LoadingCache<File, Promise<FileChannel>> CHANNELS = CacheBuilder.newBuilder()
            .expireAfterAccess(Duration.ofMinutes(1))
            .softValues()
            .removalListener((RemovalListener<File, Promise<FileChannel>>) (notification) -> {
                FileChannel channel = notification.getValue().join();
                Promise.PromiseRunnable close = channel::close;
                Cleaner.create(channel, close);
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
                     }
            );

    MemoryMapUnit SHARE_MMU = new MemoryMapUnit() {
    };

    static MemoryMapUnit shared() {
        return SHARE_MMU;
    }

    default Promise<FileChannel> channel(File file) {
        return CHANNELS.getUnchecked(file);
    }

    default Promise<ByteBuffer> map(File file, long offset, long size) {
        return channel(file).transform((channel) -> {
            return channel.map(FileChannel.MapMode.READ_WRITE, offset, size);
        });
    }

    default Promise<?> unmap(ByteBuffer buffer) {
        if (buffer.isDirect()) {
            LightReflect.share().invoke(CLEAN_DIRECT_BUFFER, buffer);
        }

        return Promise.success(null);
    }
}

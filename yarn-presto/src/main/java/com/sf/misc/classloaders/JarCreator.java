package com.sf.misc.classloaders;

import com.google.common.collect.Collections2;
import com.google.common.collect.Sets;
import com.google.common.primitives.Bytes;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.sf.misc.async.ExecutorServices;
import org.apache.commons.collections.KeyValue;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;

public class JarCreator {

    protected Set<Class<?>> classes;

    public JarCreator() {
        this.classes = Sets.newConcurrentHashSet();
    }

    public JarCreator add(Class<?> clazz) {
        this.classes.add(clazz);
        return this;
    }

    public JarCreator write(OutputStream ouput) throws IOException {
        try (JarOutputStream stream = new JarOutputStream(ouput);) {
            this.classes.parallelStream() //
                    .map((clazz) -> { //
                        JarEntry entry = new JarEntry(clazz.getName().replace(".", "/") + ".class");
                        SettableFuture<ExecutorServices.Lambda> lambda = SettableFuture.create();
                        ((ExecutorServices.Lambda) () -> {
                            ByteBuffer buffer = ByteBuffer.allocate(128);
                            ReadableByteChannel channel = Channels.newChannel( //
                                    ClassResolver.locate(clazz).get().openStream() //
                            );

                            // read fully
                            while (channel.read(buffer) != -1) {
                                if (buffer.hasRemaining()) {
                                    continue;
                                } else {
                                    buffer.flip();
                                    ByteBuffer new_buffer = ByteBuffer.allocate(buffer.limit() * 2);
                                    new_buffer.put(buffer);
                                    buffer = new_buffer;
                                }
                            }

                            buffer.flip();

                            ByteBuffer seal = buffer;
                            lambda.set(() -> {
                                stream.putNextEntry(entry);
                                stream.write(seal.array(), 0, seal.limit());
                            });
                        }).run();

                        return lambda;
                    }) //
                    .sequential() //
                    .forEach((future) -> {
                        ((ExecutorServices.Lambda) () -> future.get().run()).run();
                    });
        }
        return this;
    }
}

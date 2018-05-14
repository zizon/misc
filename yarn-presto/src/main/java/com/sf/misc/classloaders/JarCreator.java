package com.sf.misc.classloaders;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import com.sf.misc.async.ExecutorServices;
import io.airlift.log.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Map;
import java.util.function.Supplier;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

public class JarCreator {

    public static final Logger LOGGER = Logger.get(JarCreator.class);

    protected Map<String, Supplier<ByteBuffer>> entrys;
    protected Manifest manifest;

    public JarCreator() {
        this.entrys = Maps.newConcurrentMap();
        this.manifest = new Manifest();
        this.manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    }

    public JarCreator add(Class<?> clazz) {
        this.add(clazz.getName().replace(".", "/") + ".class", new Supplier<ByteBuffer>() {
            @Override
            public ByteBuffer get() {
                try {
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
                    return buffer;
                } catch (IOException e) {
                    throw new UncheckedIOException("fail to read class resource:" + clazz, e);
                }
            }
        });

        return this;
    }

    public JarCreator add(String path, Supplier<ByteBuffer> content) {
        this.entrys.put(path, content);
        return this;
    }

    public JarCreator manifest(String name, String value) {
        this.manifest.getMainAttributes().putValue(name, value);
        return this;
    }

    public JarCreator write(OutputStream ouput) throws IOException {
        try (JarOutputStream stream = new JarOutputStream(ouput, this.manifest);) {
            this.entrys.entrySet().parallelStream()
                    .map((entry) -> {
                        return new Map.Entry<JarEntry, ByteBuffer>() {
                            @Override
                            public JarEntry getKey() {
                                return new JarEntry(entry.getKey());
                            }

                            @Override
                            public ByteBuffer getValue() {
                                return entry.getValue().get();
                            }

                            @Override
                            public ByteBuffer setValue(ByteBuffer value) {
                                throw new UnsupportedOperationException();
                            }
                        };
                    })
                    .sequential()
                    .forEach((entry) -> {
                        try {
                            stream.putNextEntry(entry.getKey());

                            ByteBuffer buffer = entry.getValue();
                            stream.write(buffer.array(), 0, buffer.limit());
                        } catch (IOException e) {
                            throw new UncheckedIOException("fail to write jar entry:" + entry.getKey(), e);
                        }
                    });
        }
        return this;
    }
}

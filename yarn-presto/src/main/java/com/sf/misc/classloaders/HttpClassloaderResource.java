package com.sf.misc.classloaders;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterators;
import com.sf.misc.async.ListenablePromise;
import com.sf.misc.async.Promises;
import com.sun.org.apache.bcel.internal.generic.NEW;
import io.airlift.log.Logger;
import jdk.nashorn.internal.runtime.options.Option;
import org.apache.hive.com.esotericsoftware.kryo.io.ByteBufferInput;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.ServerErrorException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Path("/v1/http-classloader")
public class HttpClassloaderResource {
    public static Logger LOGGER = Logger.get(HttpClassloaderResource.class);

    protected static final String CLASS_FILE_POSTFIX = ".class";
    protected static final String SERVICE_PLUGIN = "META-INF/services/";

    protected LoadingCache<String, ListenablePromise<Optional<ByteBuffer>>> cachees;
    protected LoadingCache<String, ByteBuffer> service_plugin_caches;

    @Inject
    public HttpClassloaderResource(HttpClassloaderConfig config) {
        this.cachees = CacheBuilder.newBuilder() //
                .expireAfterAccess( //
                        config.getClassCacheExpire().toMillis(), //
                        TimeUnit.MILLISECONDS //
                ).build(new CacheLoader<String, ListenablePromise<Optional<ByteBuffer>>>() {
                    @Override
                    public ListenablePromise<Optional<ByteBuffer>> load(String clazz) throws Exception {
                        return Promises.submit(() -> ClassResolver.resource(clazz))
                                .transform((optional) -> {
                                    if (!optional.isPresent()) {
                                        return Optional.empty();
                                    }

                                    int chunk = 1024;
                                    ByteBuffer buffer = ByteBuffer.allocate(chunk);
                                    try (ReadableByteChannel channel = Channels.newChannel(optional.get().openConnection().getInputStream())) {
                                        // read fully
                                        while (channel.read(buffer) != -1) {
                                            if (buffer.hasRemaining()) {
                                                continue;
                                            } else {
                                                buffer.flip();

                                                ByteBuffer new_buffer = ByteBuffer.allocate(buffer.limit() + chunk);
                                                new_buffer.put(buffer);
                                                buffer = new_buffer;
                                            }
                                        }

                                        buffer.flip();
                                    }

                                    return Optional.of(buffer);
                                });
                    }
                });

        this.service_plugin_caches = CacheBuilder.newBuilder() //
                .expireAfterAccess(
                        config.getClassCacheExpire().toMillis(), //
                        TimeUnit.MILLISECONDS //
                ).build(new CacheLoader<String, ByteBuffer>() {
                    @Override
                    public ByteBuffer load(String path) throws Exception {
                        // service plugins
                        String plugin = path.substring(SERVICE_PLUGIN.length());

                        // find all service plugin urls
                        Enumeration<URL> urls = Thread.currentThread().getContextClassLoader().getResources(path);

                        String plugins = StreamSupport.stream(new Iterable<URL>() {
                            @Override
                            public Iterator<URL> iterator() {
                                return Iterators.forEnumeration(urls);
                            }
                        }.spliterator(), true)
                                .map((url) -> {
                                    // read plugin defination
                                    try (ReadableByteChannel channel = Channels.newChannel(url.openConnection().getInputStream())) {
                                        ByteBuffer buffer = ByteBuffer.allocate(1024);
                                        while (channel.read(buffer) != -1) {
                                            if (!buffer.hasRemaining()) {
                                                ByteBuffer resized = ByteBuffer.allocate(buffer.capacity() + 1024);
                                                buffer.flip();
                                                resized.put(buffer);

                                                buffer = resized;
                                            }
                                        }

                                        buffer.flip();
                                        return new String(buffer.array(), 0, buffer.limit());
                                    } catch (IOException ioexception) {
                                        throw new UncheckedIOException(ioexception);
                                    }
                                })
                                // combine
                                .collect(Collectors.joining("\n"));
                        return ByteBuffer.wrap(plugins.getBytes());
                    }
                });
    }

    @GET
    @Path("{path:.*}")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public InputStream index(@PathParam("path") String path) {
        ByteBuffer buffer = null;
        if (path.startsWith(SERVICE_PLUGIN)) {
            buffer = service_plugin_caches.getUnchecked(path);
        } else {
            buffer = this.cachees.getUnchecked(path).unchecked() //
                    .orElseThrow(() -> new NotFoundException("not found:" + path));
        }

        return new ByteArrayInputStream(buffer.array(), 0, buffer.limit());
    }
}

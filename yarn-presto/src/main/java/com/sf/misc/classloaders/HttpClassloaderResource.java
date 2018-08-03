package com.sf.misc.classloaders;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterators;
import io.airlift.log.Logger;
import jdk.internal.util.xml.impl.Input;
import org.apache.hadoop.io.IOUtils;

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
import java.util.ServiceLoader;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Path("/v1/http-classloader")
public class HttpClassloaderResource {
    public static Logger LOGGER = Logger.get(HttpClassloaderResource.class);

    protected static final String CLASS_FILE_POSTFIX = ".class";
    protected static final String SERVICE_PLUGIN = "META-INF/services/";

    protected LoadingCache<String, Optional<URL>> cachees;
    protected LoadingCache<String, byte[]> service_plugin_caches;

    @Inject
    public HttpClassloaderResource(HttpClassloaderConfig config) {
        this.cachees = CacheBuilder.newBuilder() //
                .expireAfterAccess( //
                        config.getClassCacheExpire().toMillis(), //
                        TimeUnit.MILLISECONDS //
                ).build(CacheLoader.from(ClassResolver::resource));

        this.service_plugin_caches = CacheBuilder.newBuilder() //
                .expireAfterAccess(
                        config.getClassCacheExpire().toMillis(), //
                        TimeUnit.MILLISECONDS //
                ).build(new CacheLoader<String, byte[]>() {
                    @Override
                    public byte[] load(String path) throws Exception {
                        // service plugins
                        String plugin = path.substring(SERVICE_PLUGIN.length());
                        try {
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
                            return plugins.getBytes();
                        } catch (IOException e) {
                            throw new NotFoundException("not found:" + path);
                        }
                    }
                });
    }

    @GET
    @Path("{path:.*}")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public InputStream index(@PathParam("path") String path) {
        if (path.startsWith(SERVICE_PLUGIN)) {
            return new ByteArrayInputStream(service_plugin_caches.getUnchecked(path));
        }

        Optional<URL> located = this.cachees.getUnchecked(path);
        if (located.isPresent()) {
            try {
                return located.get().openConnection().getInputStream();
            } catch (IOException e) {
                throw new ServerErrorException(Response.Status.SERVICE_UNAVAILABLE, e);
            }
        } else {
            throw new NotFoundException("not found:" + path);
        }
    }
}

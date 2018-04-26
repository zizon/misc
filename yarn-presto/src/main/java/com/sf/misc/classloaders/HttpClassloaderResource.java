package com.sf.misc.classloaders;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;
import com.google.common.hash.PrimitiveSink;
import io.airlift.joni.BitSet;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import javax.inject.Inject;
import javax.swing.text.html.Option;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.ServerErrorException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

@Path("/v1/http-classloader")
public class HttpClassloaderResource {
    public static Logger LOGGER = Logger.get(HttpClassloaderResource.class);

    protected static final String CLASS_FILE_POSTFIX = ".class";

    protected LoadingCache<String, Optional<URL>> cachees;

    @Inject
    public HttpClassloaderResource(HttpClassloaderConfig config) {
        this.cachees = CacheBuilder.newBuilder() //
                .expireAfterWrite( //
                        config.getClassCacheExpire().toMillis(), //
                        TimeUnit.MILLISECONDS //
                ).build(CacheLoader.from(this::locate));
    }


    @GET
    @Path("{path:.*}")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public InputStream index(@PathParam("path") String path) {
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

    protected Optional<URL> locate(String path) {
        Optional<URL> located = null;

        if (path.endsWith(CLASS_FILE_POSTFIX)) {
            String qualified_class_name = path.substring(0, path.length() - CLASS_FILE_POSTFIX.length()) //
                    .replace("/", ".");

            // open stream
            try {
                located = ClassResolver.locate(Class.forName(qualified_class_name));
            } catch (ClassNotFoundException exception) {
                located = Optional.ofNullable(null);
            } catch (Throwable exception) {
                located = Optional.ofNullable(null);
                LOGGER.error(exception);
            }
        } else {
            located = ClassResolver.resource(path);
        }

        if (!located.isPresent()) {
            LOGGER.warn("not found:" + path);
        }

        return located;
    }

}

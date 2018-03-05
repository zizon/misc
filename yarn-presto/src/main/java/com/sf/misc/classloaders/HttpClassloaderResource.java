package com.sf.misc.classloaders;

import io.airlift.log.Logger;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.function.BiConsumer;

@Path("/v1/http-classloader")
public class HttpClassloaderResource {
    public static Logger LOGGER = Logger.get(HttpClassloaderResource.class);

    @GET
    @Path("{path:.*}")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public InputStream index(@PathParam("path") String path) throws Exception {
        String qualified_class_name = path.substring(0, path.length() - ".class".length()) //
                .replace("/", ".");

        // load
        Class<?> target = Class.forName(qualified_class_name);

        // find file path
        URL dir = target.getResource("");
        StringBuilder buffer = new StringBuilder();
        new BiConsumer<Class<?>, StringBuilder>() {
            @Override
            public void accept(Class<?> clazz, StringBuilder buffer) {
                if (clazz.getEnclosingClass() != null) {
                    this.accept(clazz.getEnclosingClass(), buffer);
                    buffer.append('$');
                }

                buffer.append(clazz.getSimpleName());
            }
        }.accept(target, buffer);
        buffer.append(".class");

        // open stream
        return new URL(dir, buffer.toString()).openConnection().getInputStream();
    }
}

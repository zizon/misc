package com.sf.misc.classloaders;

import io.airlift.log.Logger;

import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URL;
import java.util.Optional;

@Path("/v1/http-classloader")
public class HttpClassloaderResource {
    public static Logger LOGGER = Logger.get(HttpClassloaderResource.class);

    @GET
    @Path("{path:.*}")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public InputStream index(@PathParam("path") String path) throws Exception {
        LOGGER.info("load path:" + path);
        if (path.endsWith(".class")) {
            String qualified_class_name = path.substring(0, path.length() - ".class".length()) //
                    .replace("/", ".");

            // open stream
            return ClassResolver.locate(Class.forName(qualified_class_name)) //
                    .orElseThrow(() -> new NotFoundException(qualified_class_name)) //
                    .openConnection() //
                    .getInputStream();
        }

        return ClassResolver.resource(path) //
                .orElseThrow(() -> new NotFoundException(path)) //
                .openConnection() //
                .getInputStream();
    }
}

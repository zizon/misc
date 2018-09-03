package com.sf.misc.yarn;

import io.airlift.log.Logger;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.util.concurrent.locks.LockSupport;

@Path("/v1/test/echo")
public class EchoResource {

    public static final Logger LOGGER = Logger.get(EchoResource.class);

    @GET
    public String echo(String message) {
        LOGGER.info("got");
        return "hello kitty";
    }
}

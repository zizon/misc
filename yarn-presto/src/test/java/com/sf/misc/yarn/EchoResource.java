package com.sf.misc.yarn;

import javax.ws.rs.POST;
import javax.ws.rs.Path;

@Path("/v1/test/echo")
public class EchoResource {

    @POST
    public String echo(String message){
        return message;
    }
}

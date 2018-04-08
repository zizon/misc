package com.sf.misc.classloaders;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.sf.misc.yarn.HadoopConfig;
import io.airlift.configuration.ConfigBinder;

import javax.ws.rs.Path;

import static io.airlift.discovery.client.DiscoveryBinder.discoveryBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;

public class HttpClassLoaderModule implements Module {

    public static final String SERVICE_TYPE = "http-classloader";

    @Override
    public void configure(Binder binder) {
        ConfigBinder.configBinder(binder).bindConfig(HttpClassloaderConfig.class);
        discoveryBinder(binder).bindHttpAnnouncement(SERVICE_TYPE) //
                .addProperty("path", HttpClassloaderResource.class.getAnnotation(Path.class).value());
        jaxrsBinder(binder).bind(HttpClassloaderResource.class);
    }
}

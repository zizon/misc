package com.sf.misc.airlift;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.sf.misc.annotaions.ForOnYarn;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.DiscoveryModule;
import io.airlift.discovery.server.EmbeddedDiscoveryModule;
import io.airlift.event.client.HttpEventModule;
import io.airlift.http.server.HttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.jmx.JmxModule;
import io.airlift.json.JsonModule;
import io.airlift.node.NodeModule;
import org.apache.hadoop.conf.Configuration;
import org.weakref.jmx.guice.MBeanModule;

import java.util.Map;

public class Airlift {

    private ImmutableCollection.Builder<Module> builder;
    private ImmutableMap.Builder<String, String> configuration;
    private Injector injector;

    public Airlift() {
        this.builder = defaultModules();
        this.configuration = ImmutableMap.builder();
    }

    public Airlift module(Module module) {
        this.builder.add(module);
        return this;
    }

    public Airlift withConfiguration(Map<String, String> configuration) {
        this.configuration.putAll(configuration);
        return this;
    }

    public Airlift start() {
        if (this.injector != null) {
            throw new IllegalStateException("injector is not null,may already invoke start");
        }

        try {
            this.injector = new Bootstrap(this.builder.build()) //
                    .setRequiredConfigurationProperties(this.configuration.build()) //
                    .strictConfig() //
                    .initialize();
            injector.getInstance(Announcer.class).start();
        } catch (Exception e) {
            throw new RuntimeException("fail to create start due to unexpected exception", e);
        }


        return this;
    }

    public <T> T getInstance(Class<T> type) {
        return this.injector.getInstance(type);
    }

    protected ImmutableCollection.Builder<Module> defaultModules() {
        // airlift
        return new ImmutableList.Builder<Module>()
                .add(new NodeModule()) //
                .add(new HttpServerModule()) //
                .add(new HttpEventModule()) //
                .add(new JaxrsModule()) //
                .add(new JsonModule()) //
                .add(new DiscoveryModule()) //
                .add(new EmbeddedDiscoveryModule()) //
                .add(new MBeanModule()) //
                .add(new JmxModule());
    }
}

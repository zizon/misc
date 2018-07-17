package com.sf.misc.airlift;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.sf.misc.async.ListenablePromise;
import com.sf.misc.async.Promises;
import com.sf.misc.yarn.ConfigurationAware;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.DiscoveryClientConfig;
import io.airlift.discovery.client.DiscoveryModule;
import io.airlift.discovery.client.ServiceInventory;
import io.airlift.discovery.client.ServiceInventoryConfig;
import io.airlift.discovery.server.EmbeddedDiscoveryModule;
import io.airlift.event.client.HttpEventModule;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.http.server.HttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.jmx.JmxModule;
import io.airlift.json.JsonModule;
import io.airlift.node.NodeModule;
import org.weakref.jmx.JmxException;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.guice.MBeanModule;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.reflect.Field;
import java.net.URI;

public class Airlift implements ConfigurationAware<AirliftConfig> {

    private ImmutableCollection.Builder<Module> builder;
    private AirliftConfig configuration;
    private Injector injector;

    public Airlift(AirliftConfig configuration) {
        this.builder = defaultModules();
        this.configuration = configuration;
    }

    public Airlift module(Module module) {
        this.builder.add(module);
        return this;
    }


    public ListenablePromise<Airlift> start() {
        return Promises.submit(() -> {
            if (this.injector != null) {
                throw new IllegalStateException("injector is not null,may already invoke start");
            }

            this.injector = new Bootstrap(this.builder.build()) //
                    .setRequiredConfigurationProperties(AirliftPropertyTranscript.toProperties(this.configuration)) //
                    .strictConfig() //
                    .initialize();
            injector.getInstance(Announcer.class).start();

            // for dynamic port
            DiscoveryClientConfig discovery = injector.getInstance(DiscoveryClientConfig.class);
            if (discovery.getDiscoveryServiceURI() == null) {
                discovery.setDiscoveryServiceURI(injector.getInstance(HttpServerInfo.class).getHttpUri());
            }

            // patch inventory config
            ServiceInventoryConfig inventory_config = injector.getInstance(ServiceInventoryConfig.class);
            if (inventory_config.getServiceInventoryUri() == null) {
                inventory_config.setServiceInventoryUri(URI.create(discovery.getDiscoveryServiceURI().toURL().toExternalForm() + "/v1/service"));

                // set back
                ServiceInventory inventory = injector.getInstance(ServiceInventory.class);
                Field inventory_uri = inventory.getClass().getDeclaredField("serviceInventoryUri");
                inventory_uri.setAccessible(true);
                inventory_uri.set(inventory, inventory_config.getServiceInventoryUri());
            }

            return this;
        });
    }

    public <T> T getInstance(Key<T> type) {
        return this.injector.getInstance(type);
    }

    public <T> T getInstance(Class<T> type) {
        return this.injector.getInstance(Key.get(type));
    }

    @Override
    public AirliftConfig config() {
        return this.configuration;
    }

    public static class ExtendedMBeanExporter extends MBeanExporter {
        @Inject
        public ExtendedMBeanExporter(MBeanServer server) {
            super(server);
        }

        public void export(ObjectName objectName, Object object) {
            try {
                super.export(objectName, object);
            } catch (JmxException ignore) {
            }
        }
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

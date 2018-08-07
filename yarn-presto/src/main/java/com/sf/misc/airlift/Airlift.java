package com.sf.misc.airlift;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.sf.misc.airlift.federation.Federation;
import com.sf.misc.airlift.federation.FederationModule;
import com.sf.misc.async.ListenablePromise;
import com.sf.misc.async.Promises;
import com.sf.misc.classloaders.HttpClassLoaderModule;
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
import io.airlift.log.Logger;
import io.airlift.node.NodeModule;
import org.weakref.jmx.guice.MBeanModule;

import java.io.File;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

public class Airlift implements ConfigurationAware<AirliftConfig> {

    public static final Logger LOGGER = Logger.get(Airlift.class);

    protected final ImmutableCollection.Builder<Module> builder;
    protected final Map<String, String> properties;
    protected final AirliftConfig config;
    protected Injector injector;

    public Airlift(Map<String, String> properties) {
        this.properties = Maps.newConcurrentMap();
        this.properties.putAll(properties);
        this.config = AirliftPropertyTranscript.fromProperties(this.properties, AirliftConfig.class);

        // then prepare modules
        this.builder = defaultModules();
    }

    public Airlift(AirliftConfig configuration) {
        this(AirliftPropertyTranscript.toProperties(configuration));
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

            File log_config = new File(config.getLoglevel());
            // transcript config to plain key-values
            // config logs if suggested
            if (log_config.isFile()) {
                properties.put("log.levels-file", log_config.getAbsolutePath());
            }

            // bootstrap airlift
            this.injector = new Bootstrap(this.builder.build()) //
                    .setRequiredConfigurationProperties(properties) //
                    .strictConfig() //
                    .initialize();
            injector.getInstance(Announcer.class).start();

            // patch inventory config
            crackInventoryURI();

            // update classloader config
            crackClassloader();

            // start federation brocast
            injector.getInstance(Federation.class).start();

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
        return config;
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
                .add(new JmxModule())
                .add(new FederationModule());
    }

    protected void updateConfig() {
        this.properties.putAll(AirliftPropertyTranscript.toProperties(config));
    }

    protected void crackDiscoveryURI() throws Throwable {
        DiscoveryClientConfig discovery = injector.getInstance(DiscoveryClientConfig.class);

        // no discovery specify explictly,
        // point it to self
        if (discovery.getDiscoveryServiceURI() == null) {
            discovery.setDiscoveryServiceURI(injector.getInstance(HttpServerInfo.class).getHttpUri());

            this.config.setDiscovery(discovery.getDiscoveryServiceURI().toURL().toExternalForm());
            this.config.setPort(discovery.getDiscoveryServiceURI().getPort());

            // no federation set,set to self
            if (this.config.getForeignDiscovery() == null) {
                this.config.setForeignDiscovery(config.getDiscovery());
            }
            this.updateConfig();
        }
        return;
    }

    protected void crackInventoryURI() throws Throwable {
        // make inventory uri work
        ServiceInventoryConfig inventory_config = injector.getInstance(ServiceInventoryConfig.class);
        if (inventory_config.getServiceInventoryUri() == null) {
            // not set properly,try infer.

            // ensure discovery set propertly
            crackDiscoveryURI();

            // get discovery uri
            URI discovery = injector.getInstance(DiscoveryClientConfig.class).getDiscoveryServiceURI();

            // point to founded discovery service
            inventory_config.setServiceInventoryUri(URI.create(discovery.toURL().toExternalForm() + "/v1/service"));

            // set back
            ServiceInventory inventory = injector.getInstance(ServiceInventory.class);
            Field inventory_uri = inventory.getClass().getDeclaredField("serviceInventoryUri");
            inventory_uri.setAccessible(true);
            inventory_uri.set(inventory, inventory_config.getServiceInventoryUri());

            this.config.setInventory(inventory_config.getServiceInventoryUri().toURL().toExternalForm());
            this.updateConfig();
        }
    }

    protected void crackClassloader() throws Throwable {
        String classloader = config.getClassloader();
        if (classloader == null) {
            config.setClassloader(injector.getInstance(Announcer.class).getServiceAnnouncements().parallelStream()
                    // find http classloader
                    .filter((service) -> service.getType().equals(HttpClassLoaderModule.SERVICE_TYPE))
                    // build url
                    .map((service) -> {
                        Map<String, String> properties = service.getProperties();
                        URI uri = URI.create(properties.get("http-external") + properties.get("path") + "/");
                        return uri;
                    }) // should not be null
                    .findFirst().get() // to url
                    .toURL().toExternalForm()
            );

            this.updateConfig();
        }
    }
}

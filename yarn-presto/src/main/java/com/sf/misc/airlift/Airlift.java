package com.sf.misc.airlift;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.sf.misc.airlift.federation.DiscoveryUpdateModule;
import com.sf.misc.airlift.federation.FederationModule;
import com.sf.misc.airlift.liveness.LivenessModule;
import com.sf.misc.async.ListenablePromise;
import com.sf.misc.async.Promises;
import com.sf.misc.async.SettablePromise;
import com.sf.misc.classloaders.HttpClassLoaderModule;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.DiscoveryClientConfig;
import io.airlift.discovery.client.DiscoveryModule;
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
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class Airlift {

    public static final Logger LOGGER = Logger.get(Airlift.class);

    protected final ImmutableCollection.Builder<Module> builder;
    protected final Map<String, String> properties;
    protected final AirliftConfig config;
    protected final SettablePromise<AirliftConfig> effective_config;
    protected Injector injector;

    public Airlift(Map<String, String> properties) {
        this.properties = Maps.newConcurrentMap();
        this.properties.putAll(AirliftDefaultProperties.DEFAULTS);
        this.properties.putAll(defaultProperties());
        this.properties.putAll(properties);
        this.config = AirliftPropertyTranscript.fromProperties(this.properties, AirliftConfig.class);
        this.effective_config = SettablePromise.create();

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

            if (config.getLoglevelFile() != null) {
                File log_config = new File(config.getLoglevelFile());
                // transcript config to plain key-values
                // config logs if suggested
                if (log_config.exists() && log_config.isFile()) {
                    properties.put("log.levels-file", log_config.getAbsolutePath());
                }
            }

            // bootstrap airlift
            this.injector = new Bootstrap(this.builder.build()) //
                    .setRequiredConfigurationProperties(properties) //
                    .strictConfig() //
                    .initialize();
            injector.getInstance(Announcer.class).start();

            // patch inventory config
            crackDiscoveryURI();

            // update classloader config
            crackClassloader();

            // export config
            effective_config.set(config);
            return this;
        });
    }

    public <T> T getInstance(Key<T> type) {
        return this.injector.getInstance(type);
    }

    public <T> T getInstance(Class<T> type) {
        return this.injector.getInstance(Key.get(type));
    }

    public ListenablePromise<AirliftConfig> effectiveConfig() {
        return effective_config;
    }

    public ListenablePromise<Properties> logLevels() {
        String file_path = properties.getOrDefault("log.levels-file", null);
        if (file_path == null) {
            return Promises.immediate(new Properties());
        }

        // check if file exists
        File file = new File(file_path);
        if (!file.exists() || !file.isFile()) {
            LOGGER.warn("log level specified but not exists:" + file_path);
            return Promises.immediate(new Properties());
        }

        return Promises.submit(() -> {
            try (InputStream stream = new FileInputStream(file)) {
                Properties properties = new Properties();
                properties.load(stream);

                return properties;
            }
        }).logException();
    }

    protected Map<String, String> defaultProperties() {
        return Collections.emptyMap();
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
                .add(new JmxModule()) //
                .add(new FederationModule()) //
                .add(new DiscoveryUpdateModule()) //
                .add(new LivenessModule()) //
                ;
    }

    protected void crackDiscoveryURI() throws Throwable {
        DiscoveryClientConfig discovery = injector.getInstance(DiscoveryClientConfig.class);

        // no discovery specify explictly,
        // point it to self
        if (discovery.getDiscoveryServiceURI() == null) {
            // immutable listening info
            URI uri = injector.getInstance(HttpServerInfo.class).getHttpUri();

            // update discovery uri
            discovery.setDiscoveryServiceURI(uri);

            // update port
            this.config.setPort(uri.getPort());

            // no federation set,set to self
            if (this.config.getFederationURI() == null) {
                this.config.setFederationURI(uri.toURL().toExternalForm());
            }
        }

        LOGGER.info("update discovery uri to:" + discovery.getDiscoveryServiceURI());
        LOGGER.info("update federation uri to:" + this.config.getFederationURI());
        return;
    }

    protected void crackClassloader() {
        String classloader = config.getClassloader();
        if (classloader == null) {
            injector.getInstance(Announcer.class).getServiceAnnouncements().parallelStream()
                    // find http classloader
                    .filter((service) -> service.getType().equals(HttpClassLoaderModule.SERVICE_TYPE))
                    // build url
                    .map((service) -> {
                        Map<String, String> properties = service.getProperties();
                        URI uri = URI.create(properties.get("http-external") + properties.get("path") + "/");
                        return uri;
                    }) // should not be null
                    .findAny()
                    .ifPresent((Promises.UncheckedConsumer<URI>) (uri) -> {
                        config.setClassloader(uri.toURL().toExternalForm());
                    });
        }
    }
}

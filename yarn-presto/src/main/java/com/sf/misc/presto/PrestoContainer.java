package com.sf.misc.presto;

import com.facebook.presto.hadoop.HadoopNative;
import com.facebook.presto.hive.HiveHadoop2Plugin;
import com.facebook.presto.server.PrestoServer;
import com.facebook.presto.spi.Plugin;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.sf.misc.async.ListenablePromise;
import com.sf.misc.async.Promises;
import com.sf.misc.ranger.RangerAccessControlPlugin;
import com.sf.misc.yarn.ContainerLauncher;
import io.airlift.http.server.TheServlet;
import io.airlift.log.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.NativeCodeLoader;

import javax.servlet.Filter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Stream;

import static com.google.inject.multibindings.Multibinder.newSetBinder;

public class PrestoContainer {

    public static final Logger LOGGER = Logger.get(PrestoContainer.class);

    protected static File INSTALLED_PLUGIN_DIR = new File("plugin");
    protected static File CATALOG_CONFIG_DIR = new File("etc/catalog/");
    protected static File PASSWORD_AUTHENTICATOR_CONFIG = new File("etc/password-authenticator.properties");
    protected static File ACCESS_CONTORL_CONFIG = new File("etc/access-control.properties");

    public static enum PrestoProperties {
        HiveCatalogConfig("hive-catalog-presto"),
        HdfsConfig("hdfs-presto"),
        RangerConfig("ranger-presto"),
        PrestoContainerConfig("presto-container");

        private final String prefix;

        PrestoProperties(String prefix) {
            this.prefix = prefix;
        }

        public String prefix() {
            return this.prefix;
        }
    }

    public static class HadoopNativeEnablePlugin implements Plugin {
        public HadoopNativeEnablePlugin() {
            HadoopNative.requireHadoopNative();
        }
    }

    public static void main(String args[]) throws Exception {
        LOGGER.info("using classloader:" + Thread.currentThread().getContextClassLoader());
        // initialize native
        HadoopNative.requireHadoopNative();

        // a bit tricky,since plugins use different classloader,make ugi initialized
        UserGroupInformation.setConfiguration(new Configuration());

        LOGGER.info("after native hack:" + NativeCodeLoader.buildSupportsSnappy());

        // prepare config.json
        File config = new File("config.json");

        Properties system_properties = System.getProperties();

        // generate config
        try (FileOutputStream stream = new FileOutputStream(config)) {
            Properties properties = new Properties();
            system_properties.entrySet().parallelStream() //
                    .filter((entry) -> {
                        return entry.getKey().toString().startsWith(PrestoProperties.PrestoContainerConfig.prefix());
                    }) //
                    .sequential() //
                    .forEach((entry) -> {
                        properties.put( //
                                entry.getKey().toString().substring(PrestoProperties.PrestoContainerConfig.prefix().length() + 1),  //
                                entry.getValue() //
                        );
                    });

            properties.put("plugin.dir", INSTALLED_PLUGIN_DIR.getAbsolutePath());
            properties.put("catalog.config-dir", CATALOG_CONFIG_DIR.getAbsolutePath());

            File log_dir = new File(System.getenv().get(ContainerLauncher.Enviroments.CONTAINER_LOG_DIR.name()));
            properties.put("log.enable-console", "false");
            properties.put("log.path", new File(log_dir, "presto.log").getAbsolutePath());
            properties.put("http-server.log.path", new File(log_dir, "http-request.log").getAbsolutePath());

            //properties.put("http-server.authentication.type", "PASSWORD");

            properties.store(stream, "presto generated config file");
        } catch (IOException e) {
            throw new RuntimeException("fail to create config", e);
        }

        // update properties
        System.setProperty("config", config.getAbsolutePath());

        PluginBuilder plugin_builder = new PluginBuilder(INSTALLED_PLUGIN_DIR);

        // generate plugin config
        ListenablePromise<Throwable> setup_plugins = ImmutableList.of(
                setupHive(plugin_builder, system_properties),
                setupRanger(plugin_builder, system_properties),
                setupHadoopNative(plugin_builder)
        ).parallelStream() //
                .reduce((left, right) -> {
                    return left.transformAsync((left_throwable) -> {
                        return left_throwable != null ? left : right;
                    });
                }) //
                .orElse(Promises.immediate(null));

        // plugin collected,then build it
        ListenablePromise<Throwable> build_plugins = plugin_builder.build( //
                (plugins) -> {
                    return Stream.concat( //
                            plugins.parallelStream() //
                                    .filter(Predicates.not(HadoopNativeEnablePlugin.class::equals)) //
                                    .map((plugin) -> {
                                        return new AbstractMap.SimpleImmutableEntry(plugin, "1");
                                    }),
                            Stream.of(new AbstractMap.SimpleImmutableEntry(HadoopNativeEnablePlugin.class, "0"))
                    ).parallel().map((entry) -> entry);
                });

        Throwable exception = Stream.of(setup_plugins, build_plugins) //
                .reduce((left, right) -> {
                    return left.transformAsync((left_throwable) -> {
                        return left_throwable != null ? left : right;
                    });
                }) //
                .orElse(Promises.immediate(null)) //
                .unchecked();

        if (exception != null) {
            throw new RuntimeException("fail to setup plugins", exception);
        }

        new PrestoServer() {
            protected Iterable<? extends Module> getAdditionalModules() {
                return ImmutableList.of(new Module() {
                    @Override
                    public void configure(Binder binder) {
                        newSetBinder(binder, Filter.class, TheServlet.class).addBinding()
                                .to(RangerAuthenticationFilter.class).in(Scopes.SINGLETON);
                    }
                });
            }
        }.run();
    }

    protected static ListenablePromise<Throwable> setupHive(PluginBuilder builder, Properties system_properties) {
        ListenablePromise<Throwable> plugin_ready = builder.setupPlugin(HiveHadoop2Plugin.class);

        File catalog_config = new File(CATALOG_CONFIG_DIR, "hive.properties");
        File hadoop_config = new File(CATALOG_CONFIG_DIR, "core-site.xml");

        ListenablePromise<Throwable> hdfs_config_ready = builder.ensureDirectory(hadoop_config).transform((throwable) -> {
            if (throwable != null) {
                return throwable;
            }
            LOGGER.info("prepare hadoop config:" + hadoop_config);

            Configuration configuration = new Configuration();
            system_properties.entrySet().parallelStream() //
                    .filter((entry) -> entry.getKey().toString().startsWith(PrestoProperties.HdfsConfig.prefix())) //
                    .sequential() //
                    .forEach((entry) -> {
                        String key = entry.getKey().toString().substring(PrestoProperties.HdfsConfig.prefix().length() + 1);
                        String value = entry.getValue().toString();
                        configuration.set(key, value);
                    });

            // persist
            try (FileOutputStream stream = new FileOutputStream(hadoop_config)) {
                configuration.writeXml(stream);
            } catch (IOException exception) {
                return exception;
            }

            return null;
        });

        ListenablePromise<Throwable> hive_config_ready = builder.ensureDirectory(catalog_config).transform((throwable) -> {
            if (throwable != null) {
                return throwable;
            }

            LOGGER.info("prepare catalog config:" + catalog_config);
            try (FileOutputStream stream = new FileOutputStream(catalog_config)) {
                Properties properties = new Properties();

                // setup connetor name
                properties.put("connector.name", "hive-hadoop2");

                // set metastore type
                properties.put("hive.metastore", "thrift");

                properties.put("hive.compression-codec", "SNAPPY");

                properties.put("hive.config.resources", hadoop_config.getAbsolutePath());

                //properties.put("hive.hdfs.impersonation.enabled", "true");

                // copy config
                Arrays.asList(
                        "hive.metastore.uri"
                ).forEach((key) -> {
                    properties.put(key, system_properties.getProperty(getHiveCatalogConfigKey(key)));
                });

                properties.store(stream, "hive catalog config");
            } catch (IOException exception) {
                return exception;
            }

            return null;
        });

        return ImmutableList.of(plugin_ready, hdfs_config_ready, hive_config_ready).stream().reduce((left, right) -> {
            return left.transformAsync((throwable) -> {
                if (throwable != null) {
                    return left;
                }
                return right;
            });
        }).orElse(null);
    }

    protected static ListenablePromise<Throwable> setupRanger(PluginBuilder builder, Properties system_properties) {
        ListenablePromise<Throwable> plugin_ready = builder.setupPlugin(RangerAccessControlPlugin.class);

        ListenablePromise<Throwable> access_control_config_ready = builder.ensureDirectory(ACCESS_CONTORL_CONFIG).transform((throwable) -> {
            if (throwable != null) {
                return throwable;
            }

            LOGGER.info("prepare ranger acl config:" + ACCESS_CONTORL_CONFIG);
            try (FileOutputStream stream = new FileOutputStream(ACCESS_CONTORL_CONFIG)) {
                Properties properties = new Properties();
                properties.put("access-control.name", "ranger");

                system_properties.entrySet().parallelStream() //
                        .filter((entry) -> entry.getKey().toString().startsWith(PrestoProperties.RangerConfig.prefix())) //
                        .forEach((entry) -> {
                            properties.put(
                                    entry.getKey().toString().substring(PrestoProperties.RangerConfig.prefix().length() + 1),//
                                    entry.getValue() //
                            );
                        });
                properties.store(stream, "use ranger access control");
            } catch (IOException exception) {
                return exception;
            }
            return null;
        });

        return ImmutableList.of(plugin_ready, access_control_config_ready).stream().reduce((left, right) -> {
            return left.transformAsync((throwable) -> {
                if (throwable != null) {
                    return left;
                }
                return right;
            });
        }).orElse(null);
    }

    protected static ListenablePromise<Throwable> setupHadoopNative(PluginBuilder builder) {
        return builder.setupPlugin(HadoopNativeEnablePlugin.class);
    }

    public static String getHiveCatalogConfigKey(String key) {
        return PrestoProperties.HiveCatalogConfig.prefix() + "." + key;
    }

    public static String getHdfsConfigKey(String key) {
        return PrestoProperties.HdfsConfig.prefix() + "." + key;
    }

    public static String getRangerConfigKey(String key) {
        return PrestoProperties.RangerConfig.prefix() + "." + key;
    }

    public static String getPrestoContainerConfig(String key) {
        return PrestoProperties.PrestoContainerConfig.prefix() + "." + key;
    }
}

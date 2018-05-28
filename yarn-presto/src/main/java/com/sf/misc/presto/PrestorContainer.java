package com.sf.misc.presto;

import com.facebook.presto.hive.HiveHadoop2Plugin;
import com.facebook.presto.server.PrestoServer;
import com.facebook.presto.server.security.AuthenticationFilter;
import com.facebook.presto.spi.Plugin;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.sf.misc.async.ExecutorServices;
import com.sf.misc.classloaders.JarCreator;
import com.sf.misc.yarn.ContainerLauncher;
import com.sf.misc.yarn.KickStart;
import io.airlift.http.server.TheServlet;
import io.airlift.log.Logger;
import org.apache.hadoop.conf.Configuration;

import javax.servlet.Filter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Properties;
import java.util.jar.Attributes;

import static com.google.inject.multibindings.Multibinder.newSetBinder;

public class PrestorContainer {

    public static final Logger LOGGER = Logger.get(PrestorContainer.class);

    protected static File INSTALLED_PLUGIN_DIR = new File("plugin");
    protected static File CATALOG_CONFIG_DIR = new File("etc/catalog/");
    protected static File PASSWORD_AUTHENTICATOR_CONFIG = new File("etc/password-authenticator.properties");

    public static final String YARN_PRESTO_PROPERTIES_PREFIX = "yarn-presto";
    public static final String HDFS_PRESTO_PROPERTIES_PREFIX = "hdfs-presto";

    public static void main(String args[]) throws Exception {
        // prepare config.json
        File config = new File("config.json");

        Properties system_properties = System.getProperties();

        // generate config
        try (FileOutputStream stream = new FileOutputStream(config)) {
            Properties properties = system_properties.entrySet().stream().filter((entry) -> {
                String key = entry.getKey().toString();
                return !( //
                        key.startsWith(YARN_PRESTO_PROPERTIES_PREFIX) //
                                && key.startsWith(HDFS_PRESTO_PROPERTIES_PREFIX) //
                );
            }).collect(
                    () -> new Properties(),
                    (container, entry) -> {
                        //container.put(entry.getKey(), entry.getValue());
                    },
                    (left, right) -> {
                        left.putAll(right);
                    }
            );

            properties.put("plugin.dir", INSTALLED_PLUGIN_DIR.getAbsolutePath());
            properties.put("catalog.config-dir", CATALOG_CONFIG_DIR.getAbsolutePath());

            properties.put("log.enable-console", "false");
            properties.put("log.path", new File(
                            System.getenv().get(ContainerLauncher.Enviroments.CONTAINER_LOG_DIR.name()), //
                            "presto.log" //
                    ).getAbsolutePath()
            );

            //properties.put("http-server.authentication.type", "PASSWORD");

            properties.store(stream, "presto generated config file");
        } catch (IOException e) {
            throw new RuntimeException("fail to create config", e);
        }

        // update properties
        System.setProperty("config", config.getAbsolutePath());

        // setup plugin
        Arrays.asList(
                preparePlugin(),
                prepareCatalog(system_properties)
                // preparePasswordAuthenticatorConfig()
        ).parallelStream().forEach(Futures::getUnchecked);

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

    protected static ListenableFuture<?> preparePlugin() {
        return ExecutorServices.executor().submit(() -> {
            // setup plugin
            Arrays.asList(
                    HiveHadoop2Plugin.class
                    //RangerAuthenticatorPlugin.class
            ).parallelStream().forEach((clazz) -> {
                File base = new File(INSTALLED_PLUGIN_DIR, clazz.getSimpleName());
                LOGGER.info("prepare for plugin:" + base);
                if (!base.exists() && !base.mkdirs()) {
                    throw new RuntimeException("path not found:" + base);
                }

                try (FileOutputStream stream = new FileOutputStream(new File(base, clazz.getName() + "_service.jar"))) {
                    new JarCreator() //
                            .add("META-INF/services/" + Plugin.class.getName(), //
                                    () -> ByteBuffer.wrap(clazz.getName().getBytes()) //
                            ) //
                            .manifest(Attributes.Name.CLASS_PATH.toString(), System.getenv(KickStart.HTTP_CLASSLOADER_URL)) //
                            .add(clazz) //
                            .write(stream);
                } catch (IOException e) {
                    throw new RuntimeException("fail to create service bundle for:" + clazz, e);
                }
            });
        });
    }

    protected static ListenableFuture<?> prepareCatalog(Properties system_properties) {
        File catalog_config = new File(CATALOG_CONFIG_DIR, "hive.properties");
        File hdfs_config = new File(CATALOG_CONFIG_DIR, "core-site.xml");

        // ensure dir
        Arrays.asList(
                catalog_config, //
                hdfs_config //
        ).parallelStream().forEach((file) -> {
            File parent = file.getParentFile();
            parent.mkdirs();

            if (!parent.exists() && !parent.isDirectory()) {
                throw new RuntimeException("fail to ensure path:" + parent);
            }
        });

        return ExecutorServices.executor().submit(() -> {
            Arrays.asList(
                    // hive catalog
                    (ExecutorServices.Lambda) () -> {
                        LOGGER.info("prepare catalog config:" + catalog_config);

                        try (FileOutputStream stream = new FileOutputStream(catalog_config)) {
                            Properties properties = new Properties();

                            // setup connetor name
                            properties.put("connector.name", "hive-hadoop2");

                            // set metastore type
                            properties.put("hive.metastore", "thrift");

                            properties.put("hive.compression-codec", "SNAPPY");

                            properties.put("hive.config.resources", hdfs_config.getAbsolutePath());

                            //properties.put("hive.hdfs.impersonation.enabled", "true");

                            // copy config
                            Arrays.asList(
                                    "hive.metastore.uri"
                            ).forEach((key) -> {
                                properties.put(key, system_properties.getProperty(getYarePrestoContainerConfigKey(key)));
                            });

                            properties.store(stream, "hive catalog config");
                        }
                    },

                    // hadoop config
                    (ExecutorServices.Lambda) () -> {
                        LOGGER.info("prepare hdfs config:" + hdfs_config);

                        Configuration configuration = new Configuration();
                        system_properties.entrySet().parallelStream() //
                                .filter((entry) -> entry.getKey().toString().startsWith(HDFS_PRESTO_PROPERTIES_PREFIX)) //
                                .sequential() //
                                .forEach((entry) -> {
                                    String key = entry.getKey().toString().substring(HDFS_PRESTO_PROPERTIES_PREFIX.length() + 1);
                                    String value = entry.getValue().toString();
                                    configuration.set(key, value);
                                });

                        // persist
                        try (FileOutputStream stream = new FileOutputStream(hdfs_config)) {
                            configuration.writeXml(stream);
                        }
                    }
            ).parallelStream().forEach(ExecutorServices.Lambda::run);
        });
    }

    protected static ListenableFuture<?> preparePasswordAuthenticatorConfig() {
        return ExecutorServices.executor().submit((ExecutorServices.Lambda) () -> {
            try (OutputStream stream = new FileOutputStream(PASSWORD_AUTHENTICATOR_CONFIG)) {
                Properties properties = new Properties();

                properties.put("password-authenticator.name", RangerAuthenticatorPlugin.AUTHENTICATOR_NAME);

                properties.store(stream, "password authenticator config");
            }
        });
    }

    public static String getYarePrestoContainerConfigKey(String key) {
        return YARN_PRESTO_PROPERTIES_PREFIX + "." + key;
    }

    public static String getHdfsPrestoContainerConfigKey(String key) {
        return HDFS_PRESTO_PROPERTIES_PREFIX + "." + key;
    }
}

package com.sf.misc.presto;

import com.facebook.presto.hive.HiveHadoop2Plugin;
import com.facebook.presto.server.PrestoServer;
import com.facebook.presto.spi.Plugin;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Module;
import com.sf.misc.async.ExecutorServices;
import com.sf.misc.classloaders.JarCreator;
import com.sf.misc.yarn.KickStart;
import io.airlift.log.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Properties;
import java.util.jar.Attributes;

public class PrestorContainer {

    public static final Logger LOGGER = Logger.get(PrestorContainer.class);

    protected static File INSTALLED_PLUGIN_DIR = new File("plugin");
    protected static File CATALOG_CONFIG_DIR = new File("etc/catalog/");

    public static final String YARN_PRESTO_PROPERTIES_PREFIX = "yarn-presto";

    public static void main(String args[]) throws Exception {
        // prepare config.json
        File config = new File("config.json");

        Properties system_properties = System.getProperties();

        // generate config
        try (FileOutputStream stream = new FileOutputStream(config)) {
            Properties properties = system_properties.entrySet().stream().filter((entry) -> {
                return !entry.getKey().toString().startsWith(YARN_PRESTO_PROPERTIES_PREFIX);
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
        ).stream().parallel().forEach(Futures::getUnchecked);

        new PrestoServer() {
            protected Iterable<? extends Module> getAdditionalModules() {
                return ImmutableList.of();
            }
        }.run();
    }

    protected static ListenableFuture<?> preparePlugin() {
        return ExecutorServices.executor().submit(() -> {
            // setup plugin
            Arrays.asList(
                    HiveHadoop2Plugin.class
            ).stream().parallel().forEach((clazz) -> {
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
        return ExecutorServices.executor().submit(() -> {
            Arrays.asList(
                    // hive catalog
                    (ExecutorServices.Lambda) () -> {
                        File config = new File(CATALOG_CONFIG_DIR, "hive.properties");

                        File parent = config.getParentFile();
                        if (!parent.exists() && !parent.mkdirs()) {
                            throw new RuntimeException("fail to ensure path:" + parent);
                        }

                        try (FileOutputStream stream = new FileOutputStream(config)) {
                            Properties properties = new Properties();

                            // set metastore type
                            properties.put("hive.metastore","thrift");

                            // copy config
                            Arrays.asList(
                                    "hive.metastore.uri"
                            ).forEach((key) -> {
                                properties.put(key, system_properties.getProperty(getYarePrestoContainerConfigKey(key)));
                            });

                            properties.store(stream, "hive catalog config");
                        }
                    }
            ).parallelStream().forEach(ExecutorServices.Lambda::run);
        });
    }

    public static String getYarePrestoContainerConfigKey(String key){
        return YARN_PRESTO_PROPERTIES_PREFIX + "." + key;
    }
}

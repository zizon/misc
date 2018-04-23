package com.sf.misc.presto;

import com.facebook.presto.hive.HiveHadoop2Plugin;
import com.facebook.presto.server.PrestoServer;
import com.facebook.presto.spi.Plugin;
import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import com.sf.misc.classloaders.JarCreator;
import com.sf.misc.yarn.KickStart;
import io.airlift.log.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.locks.LockSupport;
import java.util.jar.Attributes;

public class PrestorContainer {

    public static final Logger LOGGER = Logger.get(PrestorContainer.class);

    public static void main(String args[]) throws Exception {
        // prepare config.json
        File config = new File("config.json");

        // generate config
        try (FileOutputStream stream = new FileOutputStream(config)) {
            Properties properties = new Properties(System.getProperties());

            properties.store(stream, "presto generated config file");
        } catch (IOException e) {
            throw new RuntimeException("fail to create config", e);
        }

        // update properties
        System.setProperty("config", config.getAbsolutePath());

        // setup plugin
        Arrays.asList(
                HiveHadoop2Plugin.class
        ).stream().parallel().forEach((clazz) -> {
            File base = new File("plugin/" + clazz.getSimpleName());
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

        new PrestoServer() {
            protected Iterable<? extends Module> getAdditionalModules() {
                return ImmutableList.of();
            }
        }.run();
    }

}

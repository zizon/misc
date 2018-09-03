package com.sf.misc.presto;

import com.facebook.presto.spi.Plugin;
import com.google.common.collect.Maps;
import com.sf.misc.async.Entrys;
import com.sf.misc.async.ListenablePromise;
import com.sf.misc.async.Promises;
import com.sf.misc.classloaders.JarCreator;
import com.sf.misc.yarn.launcher.KickStart;
import io.airlift.log.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.jar.Attributes;

public class PluginBuilder {

    public static final Logger LOGGER = Logger.get(PluginBuilder.class);

    protected final ListenablePromise<File> plugin_dir;
    protected final ConcurrentMap<Class<? extends Plugin>, ListenablePromise<File>> installed_plugin;
    protected final URI classloader;

    public PluginBuilder(File plugin_dir, URI classloader) {
        this.plugin_dir = ensureDirectory(new File(plugin_dir, this.getClass().getSimpleName()));

        installed_plugin = Maps.newConcurrentMap();
        this.classloader = classloader;
    }

    public ListenablePromise<File> install(Class<? extends Plugin> plugin) {
        return install(plugin, "9-");
    }

    public ListenablePromise<File> install(Class<? extends Plugin> plugin, String priority) {
        return installed_plugin.compute(plugin, (key, old) -> {
            if (old != null) {
                LOGGER.warn("plugin:" + plugin + " already exits,add a duplciated one with priority");
            }

            return this.plugin_dir.transformAsync((plugin_dir) -> {
                File plugin_jar = new File(plugin_dir, priority + "_" + key.getName() + "_service.jar");
                return ensureDirectory(plugin_jar);
            }).transform((plugin_jar) -> {
                try (FileOutputStream stream = new FileOutputStream(plugin_jar)) {
                    new JarCreator() //
                            .add("META-INF/services/" + Plugin.class.getName(), //
                                    () -> ByteBuffer.wrap(key.getName().getBytes()) //
                            ) //
                            .manifest(Attributes.Name.CLASS_PATH.toString(), classloader.toURL().toExternalForm()) //
                            .add(key) //
                            .write(stream);

                    return plugin_jar;
                }
            });
        });
    }

    public ListenablePromise<File> ensureDirectory(File file) {
        return Promises.submit(() -> {
            File parent = file.getParentFile();
            parent.mkdirs();

            if (!parent.exists() && !parent.isDirectory()) {
                throw new RuntimeException("fail to ensure path:" + parent);
            }

            return file;
        });
    }


}

package com.sf.misc.presto;

import com.facebook.presto.spi.Plugin;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.sf.misc.async.ExecutorServices;
import com.sf.misc.classloaders.JarCreator;
import com.sf.misc.yarn.KickStart;
import io.airlift.log.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.jar.Attributes;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PluginBuilder {

    private static final Logger LOGGER = Logger.get(PluginBuilder.class);

    protected Set<Class<? extends Plugin>> plugins;
    protected SettableFuture<Throwable> build_final;
    protected File plugin_dir;

    public PluginBuilder(File plugin_dir) {
        this.plugins = Sets.newConcurrentHashSet();
        this.build_final = SettableFuture.create();
        this.plugin_dir = new File(Optional.of(plugin_dir).get(), this.getClass().getSimpleName());

        Futures.addCallback(ExecutorServices.executor().submit( //
                () -> {
                    this.plugin_dir.mkdirs();
                    if (!this.plugin_dir.isDirectory()) {
                        return new IllegalArgumentException("plugin dir:" + plugin_dir + " is not a directory");
                    }
                    return null;
                }), //
                new FutureCallback<Throwable>() {
                    @Override
                    public void onSuccess(Throwable result) {
                        this.onFailure(result);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        if (t != null) {
                            build_final.set(t);
                        }
                    }
                }, ExecutorServices.executor());
    }

    public ListenableFuture<Throwable> ensureDirectory(File file) {
        return ExecutorServices.executor().submit(() -> {
            File parent = file.getParentFile();
            parent.mkdirs();

            if (!parent.exists() && !parent.isDirectory()) {
                return new RuntimeException("fail to ensure path:" + parent);
            }

            return null;
        });
    }

    public ListenableFuture<Throwable> setupPlugin(Class<? extends Plugin> plugin) {
        if (build_final.isDone()) {
            throw new IllegalStateException("plugin had already built", Futures.getUnchecked(build_final));
        }

        this.plugins.add(plugin);
        return build_final;
    }

    public ListenableFuture<Throwable> build() {
        Throwable final_throwable = plugins.parallelStream().map((plugin) -> {
            try (FileOutputStream stream = new FileOutputStream(new File(plugin_dir, plugin.getName() + "_service.jar"))) {
                new JarCreator() //
                        .add("META-INF/services/" + Plugin.class.getName(), //
                                () -> ByteBuffer.wrap(plugin.getName().getBytes()) //
                        ) //
                        .manifest(Attributes.Name.CLASS_PATH.toString(), System.getenv(KickStart.HTTP_CLASSLOADER_URL)) //
                        .add(plugin) //
                        .write(stream);
            } catch (IOException e) {
                return new RuntimeException("fail to create service bundle for:" + plugin, e);
            }


            return null;
        }).filter((throwable) -> throwable != null).findAny().orElse(null);

        Function<File, Stream<File>> find_files = new Function<File, Stream<File>>() {
            @Override
            public Stream<File> apply(File file) {
                if (file.isFile()) {
                    return Stream.of(file);
                }

                return Arrays.stream(file.listFiles()).flatMap((child) -> {
                    if (child.isFile()) {
                        return Stream.of(child);
                    }

                    return this.apply(child);
                });
            }
        };

        LOGGER.info(Arrays.stream(new String[]{ //
                "generate jar files for plugin ...", //
                find_files.apply(plugin_dir).map(File::getAbsolutePath).collect(Collectors.joining("\n"))
        }).collect(Collectors.joining("\n")));

        build_final.set(final_throwable);

        return build_final;
    }
}

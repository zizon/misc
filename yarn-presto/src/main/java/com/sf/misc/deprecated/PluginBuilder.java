package com.sf.misc.deprecated;

import com.facebook.presto.spi.Plugin;
import com.google.common.base.Predicates;
import com.google.common.collect.Sets;
import com.sf.misc.async.Entrys;
import com.sf.misc.async.ListenablePromise;
import com.sf.misc.async.Promises;
import com.sf.misc.async.SettablePromise;
import io.airlift.log.Logger;

import java.io.File;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

public class PluginBuilder {

    private static final Logger LOGGER = Logger.get(PluginBuilder.class);

    protected Set<Class<? extends Plugin>> plugins;
    protected SettablePromise<Throwable> build_final;
    protected File plugin_dir;

    public PluginBuilder(File plugin_dir) {
        this.plugins = Sets.newConcurrentHashSet();
        this.build_final = SettablePromise.create();
        this.plugin_dir = new File(Optional.of(plugin_dir).get(), this.getClass().getSimpleName());

        Promises.submit(
                () -> {
                    this.plugin_dir.mkdirs();
                    if (!this.plugin_dir.isDirectory()) {
                        return new IllegalArgumentException("plugin dir:" + plugin_dir + " is not a directory");
                    }
                    return null;
                }
        ).callback((exception, throwable) -> {
            Optional<Throwable> fail = Stream.of(exception, throwable).filter(Predicates.notNull()).findAny();
            if (fail.isPresent()) {
                build_final.setException(fail.get());
            }
        });
    }

    public ListenablePromise<Throwable> ensureDirectory(File file) {
        return Promises.submit(() -> {
            File parent = file.getParentFile();
            parent.mkdirs();

            if (!parent.exists() && !parent.isDirectory()) {
                return new RuntimeException("fail to ensure path:" + parent);
            }

            return null;
        });
    }

    public ListenablePromise<Throwable> setupPlugin(Class<? extends Plugin> plugin) {
        if (build_final.isDone()) {
            Throwable throwable = build_final.unchecked();
            throw new IllegalStateException("plugin had already built:" + throwable, throwable);
        }

        this.plugins.add(plugin);
        return build_final;
    }

    public ListenablePromise<Throwable> build() {
        return build((plugins) -> plugins.parallelStream().map((plugin) -> {
            return Entrys.newImmutableEntry(plugin, "");
        }));
    }

    public ListenablePromise<Throwable> build(Function<Set<Class<? extends Plugin>>, Stream<Map.Entry<Class<? extends Plugin>, String>>> ordered) {
        /*
        Throwable final_throwable = ordered.apply(plugins).map((entry) -> {
            Class<? extends Plugin> plugin = entry.getKey();
            String order = entry.getValue();
            try (FileOutputStream stream = new FileOutputStream(new File(plugin_dir, order + "_" + plugin.getName() + "_service.jar"))) {
                new JarCreator() //
                        .add("META-INF/services/" + Plugin.class.getName(), //
                                () -> ByteBuffer.wrap(plugin.getName().getBytes()) //
                        ) //
                        .manifest(Attributes.Name.CLASS_PATH.toString(), System.getenv(KickStart.HTTP_CLASSLOADER_URL)) //
                        .add(plugin) //
                        .write(stream);
            } catch (IOException e) {
                return new IOException("fail to create service bundle for:" + plugin, e);
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


        LOGGER.info(ImmutableList.of( //
                "generate jar files for plugin ...", //
                find_files.apply(plugin_dir) //
                        .map(File::getAbsolutePath) //
                        .collect(Collectors.joining("\n"))
        ).stream().collect(Collectors.joining("\n")));

        build_final.set(final_throwable);
        */
        return build_final;
    }
}

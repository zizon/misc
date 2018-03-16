package com.sf.misc.classloaders;

import io.airlift.log.Logger;
import jdk.nashorn.internal.runtime.options.Option;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

public class ClassResolver {

    public static final Logger LOGGER = Logger.get(ClassResolver.class);

    public static Optional<URL> locate(Class<?> target) {
        // find file path
        URL dir = target.getResource(target.getSimpleName()+".class");
        if (dir == null){
            // try get root dir
            dir = target.getResource("");
        }

        StringBuilder buffer = new StringBuilder();
        new BiConsumer<Class<?>, StringBuilder>() {
            @Override
            public void accept(Class<?> clazz, StringBuilder buffer) {
                if (clazz.getEnclosingClass() != null) {
                    this.accept(clazz.getEnclosingClass(), buffer);
                    buffer.append('$');
                }

                buffer.append(clazz.getSimpleName());
            }
        }.accept(target, buffer);
        buffer.append(".class");

        // open stream
        try {
            return Optional.of(new URL(dir, buffer.toString()));
        } catch (MalformedURLException e) {
            LOGGER.error(e, "fail to locate class location:" + target.getName());
            return Optional.empty();
        }
    }
}

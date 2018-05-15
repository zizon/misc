package com.sf.misc.classloaders;

import io.airlift.log.Logger;
import jdk.nashorn.internal.runtime.options.Option;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.function.BiConsumer;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

public class ClassResolver {

    public static final Logger LOGGER = Logger.get(ClassResolver.class);

    public static Optional<URL> locate(Class<?> target) {
        return resource(target.getName().replace(".", "/") + ".class");
    }

    public static Optional<URL> resource(String resource_name) {
        return Optional.ofNullable( //
                Thread.currentThread() //
                        .getContextClassLoader() //
                        .getResource(resource_name) //
        );
    }
}

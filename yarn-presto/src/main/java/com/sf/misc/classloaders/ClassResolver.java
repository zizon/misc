package com.sf.misc.classloaders;

import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import io.airlift.log.Logger;

import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Optional;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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

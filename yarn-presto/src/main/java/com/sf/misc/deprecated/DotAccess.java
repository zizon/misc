package com.sf.misc.deprecated;

import com.facebook.presto.hive.$internal.jodd.exception.UncheckedException;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import java.util.stream.Collectors;

public class DotAccess {

    protected static Cache<Class<?>, ConcurrentMap<String, Function<Object, Object>>> FIELD_CACHE = CacheBuilder.newBuilder() //
            .expireAfterAccess(1, TimeUnit.MINUTES) //
            .build(new CacheLoader<Class<?>, ConcurrentMap<String, Function<Object, Object>>>() {
                @Override
                public ConcurrentMap<String, Function<Object, Object>> load(Class<?> key) throws Exception {
                    return Arrays.stream(key.getDeclaredFields()).parallel() //
                            .collect( //
                                    Collectors.<Field, String, Function<Object, Object>>toConcurrentMap( //
                                            (field) -> field.getName(), //
                                            (field) -> {
                                                if (!field.isAccessible()) {
                                                    field.setAccessible(true);
                                                }

                                                if (Modifier.isStatic(field.getModifiers())) {
                                                    return (object) -> {
                                                        try {
                                                            return field.get(null);
                                                        } catch (IllegalAccessException e) {
                                                            throw new UncheckedException(e);
                                                        }
                                                    };
                                                }
                                                return (object) -> {
                                                    try {
                                                        return field.get(object);
                                                    } catch (IllegalAccessException e) {
                                                        throw new UncheckedException(e);
                                                    }
                                                };
                                            }//
                                    ) //
                            );
                }
            });

    protected static Cache<Class<?>, ConcurrentMap<String, List<Method>>> METHOD_CACHE = CacheBuilder.newBuilder() //
            .expireAfterAccess(1, TimeUnit.MINUTES) //
            .build(new CacheLoader<Class<?>, ConcurrentMap<String, List<Method>>>() {
                @Override
                public ConcurrentMap<String, List<Method>> load(Class<?> key) throws Exception {
                    ConcurrentMap<String, List<Method>> container = Maps.newConcurrentMap();
                    Arrays.stream(key.getDeclaredMethods()).parallel() //
                            .forEach( //
                                    (method) -> {
                                        if (!method.isAccessible()) {
                                            method.setAccessible(true);
                                        }

                                        container.compute(method.getName(), (ignore, list) -> {
                                            if (list == null) {
                                                list = Lists.newArrayList();
                                            }

                                            list.add(method);
                                            return list;
                                        });
                                    } //
                            );
                    return container;
                }
            });

    public static <T> T invoke(Object object, String path, Class<T> return_type, Object[] args) {
        String[] depth = path.split(".");

        Function<Object, Object> field = FIELD_CACHE.getIfPresent(object.getClass()).get(depth[0]);
        if (field != null) {
            object = field.apply(object);
        } else {
            List<Method> methods = METHOD_CACHE.getIfPresent(object.getClass()).get(depth[0]);
            try {
                object = methods.stream().parallel()//
                        .filter((method) -> method.getParameterCount() == args.length) //
                        .findFirst().get() //
                        .invoke(object, args);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new UncheckedException(e);
            }
        }


        if (depth.length == 1) {
            return (T) object;
        }

        StringBuilder buffer = new StringBuilder();
        for (int i = 1; i < depth.length; i++) {
            buffer.append(depth[i]).append('.');
        }
        if (buffer.length() > 0) {
            buffer.setLength(buffer.length() - 1);
        }
        return invoke(object, buffer.toString(), return_type, args);
    }
}

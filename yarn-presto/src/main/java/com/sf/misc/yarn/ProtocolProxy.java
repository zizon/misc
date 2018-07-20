package com.sf.misc.yarn;

import com.google.common.collect.Maps;
import com.google.common.reflect.Reflection;
import com.sf.misc.async.Entrys;
import org.apache.hadoop.security.UserGroupInformation;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.security.PrivilegedExceptionAction;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ProtocolProxy<T> {

    protected final ConcurrentMap<String, Map.Entry<Method, Object>> method_cache;
    protected final Class<T> protocol;

    public ProtocolProxy(Class<T> protocol, Object[] delegates) {
        this.protocol = protocol;
        method_cache = Stream.of(delegates) //
                .parallel().flatMap((instance) -> {
                    return findInterfaces(instance.getClass()).parallelStream() //
                            .flatMap((iface) -> {
                                return Arrays.stream(iface.getMethods());
                            }).map((method) -> {
                                return Entrys.newImmutableEntry(method.getName(), new AbstractMap.SimpleImmutableEntry<>(method, instance));
                            });
                }).collect(
                        Maps::newConcurrentMap, //
                        (map, entry) -> {
                            map.put(entry.getKey(), entry.getValue());
                        }, //
                        Map::putAll
                );
    }

    public T make() {
        return Reflection.newProxy(this.protocol, new InvocationHandler() {
            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                Map.Entry<Method, Object> method_instance = method_cache.get(method.getName());
                return method_instance.getKey()//
                        .invoke(method_instance.getValue(), args);
            }
        });
    }

    public T make(UserGroupInformation ugi) {
        return Reflection.newProxy(this.protocol, new InvocationHandler() {
            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                Map.Entry<Method, Object> method_instance = method_cache.get(method.getName());
                return ugi.doAs( //
                        (PrivilegedExceptionAction<Object>) () -> method_instance.getKey()//
                                .invoke(method_instance.getValue(), args)
                );
            }
        });
    }

    protected Set<Class<?>> findInterfaces(Class<?> to_infer) {
        Stream<Class<?>> indrect = Arrays.stream(to_infer.getInterfaces()) //
                .flatMap((iface) -> {
                    return findInterfaces(iface).parallelStream();
                });
        return Stream.concat(Arrays.stream(to_infer.getInterfaces()), indrect).collect(Collectors.toSet());
    }
}

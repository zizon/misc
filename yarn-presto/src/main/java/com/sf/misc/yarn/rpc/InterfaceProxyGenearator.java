package com.sf.misc.yarn.rpc;

import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.sf.misc.async.ListenablePromise;
import com.sf.misc.async.Promises;
import it.unimi.dsi.fastutil.objects.Object2ByteRBTreeMap;

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class InterfaceProxyGenearator {

    protected final ListenablePromise<Set<Method>> interfaces;
    protected final Queue<ListenablePromise<ConcurrentMap<Method, Object>>> delegates;

    public InterfaceProxyGenearator(Class<?> interface_type) {
        this.interfaces = findInteraceMethods(interface_type);
        this.delegates = Queues.newConcurrentLinkedQueue();
    }

    public InterfaceProxyGenearator bindTo(Object object) {
        this.delegates.offer( //
                findInteraceMethods(object.getClass()) //
                        .transform((methods) -> { //
                            return methods.parallelStream() //
                                    .filter(Predicates.not(Method::isSynthetic))
                                    .collect(Collectors.toConcurrentMap(
                                            (method) -> method,
                                            (method) -> object
                                    ));
                        }) //
        );
        return this;
    }

    public <T> T make() {
        this.delegates.parallelStream() //
                .map(methods -> methods.transform((instance) -> instance.entrySet()))
                .reduce(Promises.reduceCollectionsOperator())
                .orElse(Promises.immediate(Collections.emptySet()))
                .transform((methods) -> {
                    return methods.parallelStream().collect(Collectors.toConcurrentMap(
                            Map.Entry::getKey,
                            Map.Entry::getValue
                    ));
                });
        //TODO
        return null;
    }

    protected ListenablePromise<Set<Method>> findInteraceMethods(Class<?> interface_type) {
        return Promises.submit(() -> {
            Set<Method> methods = Sets.newConcurrentHashSet();
            if (interface_type.isInterface()) {
                Arrays.stream(interface_type.getDeclaredMethods()).parallel()
                        .forEach(methods::add);
            }

            return Arrays.stream(interface_type.getInterfaces()).parallel()
                    .map(this::findInteraceMethods)
                    .reduce(Promises.reduceCollectionsOperator())
                    .orElse(Promises.immediate(Sets.newConcurrentHashSet()))
                    .transform((graps) -> {
                        methods.addAll(graps);

                        return methods.parallelStream().filter((method) -> {
                            return !(method.isDefault() || method.isSynthetic() || !method.isAccessible());
                        }).collect(Collectors.toSet());
                    });
        }).transformAsync((through) -> through);
    }
}

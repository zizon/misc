package com.sf.misc.yarn.rpc;

import com.google.common.collect.Maps;
import com.google.common.reflect.Reflection;
import com.sf.misc.async.Entrys;
import com.sf.misc.async.ListenablePromise;
import com.sf.misc.async.Promises;
import io.airlift.bytecode.DynamicClassLoader;
import io.airlift.log.Logger;
import org.apache.hadoop.security.UserGroupInformation;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

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

    public static final Logger LOGGER = Logger.get(ProtocolProxy.class);

    protected static final DynamicClassLoader PROXY_GENERATOR = new DynamicClassLoader(Thread.currentThread().getContextClassLoader());

    protected final ConcurrentMap<String, Map.Entry<Method, ListenablePromise<Object>>> method_cache;
    protected final Class<T> protocol;
    protected final ListenablePromise<Object> default_instance;

    public ProtocolProxy(Class<T> protocol, Object[] delegates) {
        this.protocol = protocol;
        this.default_instance = makeDefualtInstance();

        method_cache = Stream.of(delegates) //
                .parallel().flatMap((instance) -> {
                    return findInterfaces(instance.getClass()).parallelStream() //
                            .flatMap((iface) -> {
                                return Arrays.stream(iface.getMethods());
                            }).map((method) -> {
                                return Entrys.newImmutableEntry(method.getName(), new AbstractMap.SimpleImmutableEntry<>(method, Promises.immediate(instance)));
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
        return this.make(null);
    }

    public T make(UserGroupInformation ugi) {
        return Reflection.newProxy(this.protocol, new InvocationHandler() {
            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                return invokeLogged(proxy, method, args);
            }

            protected Object invokeLogged(Object proxy, Method method, Object[] args) throws Throwable {
                Map.Entry<Method, ListenablePromise<Object>> method_instance = method_cache.compute(method.getName(), (key, old) -> {
                    if (old == null) {
                        LOGGER.warn("may be a default interface method?" + method + " try default instance:" + default_instance.unchecked());
                        old = Entrys.newImmutableEntry(method, default_instance);
                    }

                    return old;
                });

                PrivilegedExceptionAction<Object> action = () -> {
                    return method_instance.getKey()//
                            .invoke(method_instance.getValue().unchecked(), args);
                };

                if (ugi != null) {
                    return ugi.doAs(action);
                } else {
                    return action.run();
                }
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

    protected ListenablePromise<Object> makeDefualtInstance() {
        return Promises.submit(() -> {
            Type type = Type.getObjectType(("generated." + protocol.getName()).replace(".", "/"));

            ClassWriter writer = new ClassWriter(ClassWriter.COMPUTE_FRAMES);
            writer.visit(
                    Opcodes.V1_8,
                    Opcodes.ACC_PUBLIC,
                    type.getInternalName(),
                    null,
                    Type.getInternalName(Object.class),
                    new String[]{Type.getInternalName(protocol)}//
            );

            // generate constructor
            MethodVisitor method = writer.visitMethod(Opcodes.ACC_PUBLIC, "<init>", Type.getMethodDescriptor(Type.getType(void.class)), null, null);
            method.visitVarInsn(Opcodes.ALOAD, 0);
            method.visitMethodInsn(Opcodes.INVOKESPECIAL, Type.getInternalName(Object.class), "<init>", Type.getMethodDescriptor(Type.getType(void.class)), false);
            method.visitInsn(Opcodes.RETURN);
            method.visitMaxs(0, 0);
            method.visitEnd();

            writer.visitEnd();

            return PROXY_GENERATOR.defineClass(type.getClassName(), writer.toByteArray()) //
                    .newInstance();
        });
    }

}

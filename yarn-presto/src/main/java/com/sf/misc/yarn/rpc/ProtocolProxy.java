package com.sf.misc.yarn.rpc;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.reflect.Reflection;
import com.sf.misc.async.Entrys;
import com.sf.misc.async.ListenablePromise;
import com.sf.misc.async.Promises;
import com.sf.misc.bytecode.AnyCast;
import io.airlift.bytecode.DynamicClassLoader;
import io.airlift.log.Logger;
import org.apache.hadoop.security.UserGroupInformation;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class ProtocolProxy<T> {

    public static final Logger LOGGER = Logger.get(ProtocolProxy.class);

    protected final T instance;
    protected final Class<T> protocol;

    public ProtocolProxy(Class<T> protocol, Object[] delegates) {
        AnyCast any = new AnyCast();
        Arrays.stream(delegates).forEach(any::adopt);

        this.instance = any.newInstance(protocol);
        this.protocol = protocol;
    }

    public T make() {
        return this.make(null);
    }

    public T make(UserGroupInformation ugi) {
        if (ugi == null) {
            return instance;
        }

        return Reflection.newProxy(this.protocol, new InvocationHandler() {
            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                return invokeLogged(proxy, method, args);
            }

            protected Object invokeLogged(Object proxy, Method method, Object[] args) throws Throwable {
                return ugi.doAs((PrivilegedExceptionAction<Object>) () -> {
                    return method.invoke(instance, args);
                });
            }
        });
    }
}

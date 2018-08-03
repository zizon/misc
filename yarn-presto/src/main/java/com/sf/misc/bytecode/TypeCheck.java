package com.sf.misc.bytecode;

import com.google.common.base.Predicates;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.util.Arrays;

public class TypeCheck {

    public Class<?> findAccesiableClass(Class<?> clazz, String name, MethodType type) {
        if (clazz == null) {
            return null;
        }

        try {
            if (MethodHandles.lookup().findVirtual(clazz, name, type) != null) {
                return clazz;
            }
        } catch (ReflectiveOperationException e) {
        }

        // not found
        if (clazz.isAssignableFrom(Object.class)) {
            // clazz is Object.class,
            // and not found named method
            return null;
        }

        // not Object.class
        // try parent
        Class<?> found = findAccesiableClass(clazz.getSuperclass(), name, type);
        if (found != null) {
            return found;
        }

        // try interface
        return Arrays.stream(clazz.getInterfaces()).parallel() //
                .map((interface_type) -> {
                    return findAccesiableClass(interface_type, name, type);
                }) //
                .filter(Predicates.notNull()) //
                .findFirst() //
                .orElse(null);
    }

    public MethodType toType(Method method) {
        return MethodType.methodType(method.getReturnType(), method.getParameterTypes());
    }
}

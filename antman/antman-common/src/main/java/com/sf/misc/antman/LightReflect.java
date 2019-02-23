package com.sf.misc.antman;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

public interface LightReflect {

    Log LOGGER = LogFactory.getLog(LightReflect.class);

    LightReflect SHARE = new LightReflect() {
    };

    static LightReflect share() {
        return SHARE;
    }

    default MethodHandle getter(Field field) {
        field.setAccessible(true);
        try {
            return lookup().unreflectGetter(field);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("can not access getter of field:" + field, e);
        }
    }

    default MethodHandle setter(Field field) {
        field.setAccessible(true);
        try {
            return lookup().unreflectSetter(field);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("can not access setter of field:" + field, e);
        }
    }


    default Optional<MethodHandle> method(Class<?> clazz, String name, MethodType type) {
        try {
            return Optional.of(lookup().findVirtual(clazz, name, type));
        } catch (NoSuchMethodException e) {
            return Optional.empty();
        } catch (IllegalAccessException e) {
            throw new RuntimeException("unable to touch method:" + name + " in class:" + clazz + " of type:" + type, e);
        }
    }

    default Optional<MethodHandle> staticMethod(Class<?> clazz, String name, MethodType type) {
        try {
            return Optional.of(lookup().findStatic(clazz, name, type));
        } catch (NoSuchMethodException e) {
            return Optional.empty();
        } catch (IllegalAccessException e) {
            throw new RuntimeException("unable to touch method:" + name + " in class:" + clazz + " of type:" + type, e);
        }
    }

    default Object invoke(MethodHandle handle, Object... args) {
        try {
            return handle.invokeExact(args);
        } catch (Throwable throwable) {
            throw new RuntimeException("bind failed,method:" + handle
                    + " args:" + Arrays.stream(args).map(Object::toString).collect(Collectors.joining(";")),
                    throwable);
        }
    }

    default MethodHandle invokable(MethodHandle handle) {
        return handle.asSpreader(Object[].class, handle.type().parameterCount())
                .asType(MethodType.methodType(
                        Object.class,
                        Object[].class
                ));
    }

    default MethodHandle bind(MethodHandle origin, int index, Object constant) {
        return MethodHandles.insertArguments(origin, index, constant);
    }

    default MethodHandles.Lookup lookup() {
        return MethodHandles.publicLookup();
    }
}

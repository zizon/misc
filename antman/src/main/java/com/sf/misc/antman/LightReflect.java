package com.sf.misc.antman;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.lang.invoke.CallSite;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public interface LightReflect {

    public static final Log LOGGER = LogFactory.getLog(LightReflect.class);

    static LightReflect create() {
        return new LightReflect() {
        };
    }

    default Stream<Field> declaredFields(Class<?> clazz) {
        if (clazz == null) {
            return Stream.empty();
        }

        return Stream.<Stream<Field>>builder()
                .add(Arrays.stream(clazz.getDeclaredFields()))
                .add(declaredFields(clazz.getSuperclass()))
                .build()
                .flatMap(Function.identity())
                .filter((field) -> !Modifier.isStatic(field.getModifiers()))
                .filter((field) -> !field.isSynthetic())
                .sorted(Comparator.comparing(Field::getName));
    }

    default Stream<MethodHandle> declearedGetters(Class<?> clazz) {
        return declaredFields(clazz).map((field) -> {
            if (!field.isAccessible()) {
                field.setAccessible(true);
            }

            try {
                return lookup().unreflectGetter(field);
            } catch (IllegalAccessException e) {
                throw new RuntimeException("can not access declared field gettter for:" + clazz + " field:" + field, e);
            }
        });
    }

    default Stream<MethodHandle> declearedSetters(Class<?> clazz) {
        return declaredFields(clazz).map((field) -> {
            if (!field.isAccessible()) {
                field.setAccessible(true);
            }

            try {
                return lookup().unreflectSetter(field);
            } catch (IllegalAccessException e) {
                throw new RuntimeException("can not access declared field setter for:" + clazz + " field:" + field, e);
            }
        });
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
            throw new RuntimeException("unable to touch method:" + name + " in class:" + clazz + " of type:" + type);
        }
    }

    default Object invoke(MethodHandle handle, Object... args) {
        try {
            return handle.invokeExact(args);
        } catch (Throwable throwable) {
            throw new RuntimeException("bind failed", throwable);
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

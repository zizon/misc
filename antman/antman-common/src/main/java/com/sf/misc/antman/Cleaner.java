package com.sf.misc.antman;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.util.LinkedList;
import java.util.List;

public class Cleaner {

    public static final Log LOGGER = LogFactory.getLog(Cleaner.class);

    protected static final LightReflect REFLECT = LightReflect.share();
    protected static final MethodHandle CLEANER_CREATE = createCleaner();
    protected static final MethodHandle CLEANER_REGISTER = createCleanerRegister();
    protected static final MethodHandle CLEANER_CLEAN = createClean();


    protected static MethodHandle createCleaner() {
        List<Promise.PromiseRunnable> messages = new LinkedList<>();
        try {
            Class<?> jdk_9_cleaner = Class.forName("java.lang.ref.Cleaner");
            MethodHandle create = REFLECT.staticMethod(
                    jdk_9_cleaner,
                    "create",
                    MethodType.methodType(jdk_9_cleaner)
            ).orElseThrow(() -> new NoSuchMethodException("no create method for cleaner"));

            return REFLECT.invokable(create);
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            messages.add(() -> LOGGER.warn("no cleaner class found", e));
        }

        messages.forEach(Runnable::run);
        throw new RuntimeException("fail to find Cleaner create method");
    }

    protected static MethodHandle createCleanerRegister() {
        List<Promise.PromiseRunnable> messages = new LinkedList<>();
        try {
            Class<?> jdk_9_cleaner = Class.forName("java.lang.ref.Cleaner");
            Class<?> jdk_9_cleanable = Class.forName("java.lang.ref.Cleaner$Cleanable");

            MethodHandle create = REFLECT.method(
                    jdk_9_cleaner,
                    "register",
                    MethodType.methodType(
                            jdk_9_cleanable,
                            Object.class,
                            Runnable.class
                    )
            ).orElseThrow(() -> new NoSuchMethodException("no register method for cleaner"));

            return REFLECT.invokable(create);
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            messages.add(() -> LOGGER.warn("no cleaner class found", e));
        }

        messages.forEach(Runnable::run);
        throw new RuntimeException("fail to find Cleaner register method");
    }

    protected static MethodHandle createClean() {
        List<Promise.PromiseRunnable> messages = new LinkedList<>();
        try {
            Class<?> jdk_9_cleanable = Class.forName("java.lang.ref.Cleaner$Cleanable");

            MethodHandle create = REFLECT.method(
                    jdk_9_cleanable,
                    "clean",
                    MethodType.methodType(void.class)
            ).orElseThrow(() -> new NoSuchMethodException("no clean method for cleaner"));

            return REFLECT.invokable(create);
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            messages.add(() -> LOGGER.warn("no cleaner class found", e));
        }

        messages.forEach(Runnable::run);
        throw new RuntimeException("fail to find Cleaner clean method");
    }


    protected final Object real;

    protected Cleaner(Object real) {
        this.real = real;
    }

    public static Cleaner create(Object ob, Runnable thunk) {
        //return new Cleaner(CLEANER_CREATE.apply(ob, thunk));
        Object cleaner = REFLECT.invoke(CLEANER_CREATE);

        Object real = REFLECT.invoke(CLEANER_REGISTER, cleaner, ob, thunk);
        return new Cleaner(real);
    }

    public void clean() {
        REFLECT.invoke(CLEANER_CLEAN, real);
    }
}

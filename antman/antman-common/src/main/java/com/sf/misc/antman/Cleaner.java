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
    protected static final MethodHandle JDK8_CLEANABLE_CREATE = createJDK8ClenableCreate();
    protected static final MethodHandle CLEANER_CREATE = createCleaner();
    protected static final MethodHandle CLEANER_REGISTER = createCleanerRegister();
    protected static final MethodHandle CLEANER_CLEAN = createClean();


    protected static MethodHandle createCleaner() {
        List<Promise.PromiseRunnable> messages = new LinkedList<>();

        // jdk 9 case
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

        // jdk 8 case
        try {
            Class<?> cleaner = Class.forName("sun.misc.Cleaner");
            MethodHandle create = MethodHandles.constant(cleaner, null);

            return REFLECT.invokable(create);
        } catch (ClassNotFoundException e) {
            messages.add(() -> LOGGER.warn("fail to find jdk8 cleaner create", e));
        }

        messages.forEach(Runnable::run);
        throw new RuntimeException("fail to find Cleaner create method");
    }

    protected static MethodHandle createCleanerRegister() {
        List<Promise.PromiseRunnable> messages = new LinkedList<>();

        // jdk9 case
        try {
            Class<?> jdk_9_cleaner = Class.forName("java.lang.ref.Cleaner");
            Class<?> jdk_9_cleanable = Class.forName("java.lang.ref.Cleaner$Cleanable");

            MethodHandle register = REFLECT.method(
                    jdk_9_cleaner,
                    "register",
                    MethodType.methodType(
                            jdk_9_cleanable,
                            Object.class,
                            Runnable.class
                    )
            ).orElseThrow(() -> new NoSuchMethodException("no register method for cleaner"));

            return REFLECT.invokable(register);
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            messages.add(() -> LOGGER.warn("no cleaner register method found", e));
        }

        // jdk8 case
        try {
            Method target = Cleaner.class.getDeclaredMethod(
                    "jdk8CleanableRegister",
                    Object.class,
                    Object.class,
                    Runnable.class
            );
            target.setAccessible(true);

            MethodHandle jdk8_register_wrap = REFLECT.lookup().unreflect(target);

            return REFLECT.invokable(jdk8_register_wrap);
        } catch (NoSuchMethodException | IllegalAccessException e) {
            messages.add(() -> LOGGER.warn("no cleaner register method found", e));
        }

        messages.forEach(Runnable::run);
        throw new RuntimeException("fail to find Cleaner create method");
    }

    protected static void jdk8CleanableRegister(Object ignore, Object ob, Runnable thunk) {
        REFLECT.invoke(JDK8_CLEANABLE_CREATE, ob, thunk);
    }

    protected static MethodHandle createJDK8ClenableCreate() {
        try {
            Class<?> cleaner = Class.forName("sun.misc.Cleaner");

            MethodHandle create = REFLECT.staticMethod(
                    cleaner,
                    "create",
                    MethodType.methodType(
                            cleaner,
                            Object.class,
                            Runnable.class
                    )
            ).orElseThrow(() -> new NoSuchMethodException("no create method found for Cleaner"));

            return REFLECT.invokable(create);
        } catch (ClassNotFoundException e) {
            return null;
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("fail to get create method of cleaner", e);
        }
    }

    protected static MethodHandle createClean() {
        List<Promise.PromiseRunnable> messages = new LinkedList<>();

        // jdk9 case
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

        // jdk8 case
        try {
            Class<?> cleaner = Class.forName("sun.misc.Cleaner");

            MethodHandle clean = REFLECT.method(
                    cleaner,
                    "clean",
                    MethodType.methodType(void.class)
            ).orElseThrow(() -> new NoSuchMethodException("no clean method for cleaner"));

            return REFLECT.invokable(clean);
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

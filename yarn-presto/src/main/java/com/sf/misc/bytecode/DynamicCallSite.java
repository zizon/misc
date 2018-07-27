package com.sf.misc.bytecode;

import com.google.common.collect.Lists;
import io.airlift.log.Logger;

import java.lang.invoke.CallSite;
import java.lang.invoke.ConstantCallSite;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.util.Arrays;

public interface DynamicCallSite {
    public static final Logger LOGGER = Logger.get(DynamicCallSite.class);

    public static final MethodType INVOKE_DYNAMIC_METHOD_TYPE = MethodType.methodType(CallSite.class, Object.class, MethodHandles.Lookup.class, String.class, MethodType.class);
    public static final String INVOKE_DYNAMIC_METHOD = Static.callsiteName();

    static class Static {
        protected final static MethodHandle TRACER = initialize();

        protected static void printArgs(Object... args) {
            LOGGER.info("args:" + Arrays.deepToString(args));
        }

        protected static MethodHandle initialize() {
            try {
                return MethodHandles.lookup().in(Static.class).findStatic(Static.class, "printArgs", MethodType.methodType(void.class, Object[].class));
            } catch (Throwable e) {
                LOGGER.error(e, "fail to bind printArgs");
                return null;
            }
        }

        protected static String callsiteName() {
            return Arrays.stream(DynamicCallSite.class.getMethods()).parallel()
                    .filter((method) -> {
                        return INVOKE_DYNAMIC_METHOD_TYPE.equals(MethodType.methodType(method.getReturnType(), Arrays.asList(method.getParameterTypes())));
                    }) //
                    .map(Method::getName) //
                    .findAny() //
                    .orElse(null);
        }
    }

    default CallSite callsite(Object instance, MethodHandles.Lookup caller, String name, MethodType type) {
        return new ConstantCallSite(Static.TRACER);
    }
}

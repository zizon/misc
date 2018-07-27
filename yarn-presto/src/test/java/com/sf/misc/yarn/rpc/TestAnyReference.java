package com.sf.misc.yarn.rpc;

import com.sf.misc.async.ListenablePromise;
import com.sf.misc.classloaders.ClassResolver;
import io.airlift.log.Logger;
import org.junit.Test;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.util.TraceClassVisitor;

import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentMap;

public class TestAnyReference {

    public static final Logger LOGGER = Logger.get(TestAnyReference.class);

    public static class StandaloneClass implements GenericInterface<String> {
        public void standalone() {
        }

        @Override
        public String generic() {
            System.out.println("invoke standalone generic");
            return null;
        }
    }

    public static interface NoDefaultInterface {
        public void noDefault();
    }

    public static interface DefaultInterface {
        default boolean defaultMethod() {
            return true;
        }
    }

    public static interface GenericInterface<T> {
        public T generic();
    }

    public static interface CombindeInterface extends NoDefaultInterface, DefaultInterface, GenericInterface<String> {
    }

    @Test
    public void test() throws Throwable {
        AnyReference any = new AnyReference(CombindeInterface.class);
        any.adopt(new StandaloneClass());
        any.adopt(new NoDefaultInterface() {
            @Override
            public void noDefault() {
                LOGGER.info("invoke no default method");
            }
        });

        ConcurrentMap<Method, Object> delegates = any.associate().unchecked();
        delegates.forEach((method, object) -> {
            LOGGER.info("method:" + method + " object:" + object);
        });

        any.collectArgumetns(delegates).forEach((entry) -> {
            LOGGER.info("args:" + entry);
        });

        //CombindeInterface instance = any.make();
        //instance.generic();
        //printClass(StandaloneClass.class);
    }

    public void printClass(Class<?> clazz) throws Throwable {
        ClassReader reader = new ClassReader(ClassResolver.locate(clazz).get().openStream());
        reader.accept(new TraceClassVisitor(new PrintWriter(System.out)), 0);
    }
}
package com.sf.misc.yarn.rpc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.sf.misc.classloaders.ClassResolver;
import com.sun.org.apache.xalan.internal.xsltc.compiler.Template;
import io.airlift.log.Logger;
import org.junit.Test;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.util.TraceClassVisitor;

import java.io.PrintWriter;
import java.util.Arrays;

public class TestCodeGen {

    public static final Logger LOGGER = Logger.get(TestCodeGen.class);

    public void printClass(Class<?> clazz) throws Throwable {
        ClassReader reader = new ClassReader(ClassResolver.locate(clazz).get().openStream());
        reader.accept(new TraceClassVisitor(new PrintWriter(System.out)), 0);
    }

    public void printClass(byte[] clazz) throws Throwable {
        ClassReader reader = new ClassReader(clazz);
        reader.accept(new TraceClassVisitor(new PrintWriter(System.out)), 0);
    }

    public static interface Interface<T> {

        public String print(T input);

        default String defaultPrint() {
            return "default";
        }
    }

    public static class Delegate implements Interface<String> {
        @Override
        public String print(String input) {
            return input;
        }
    }

    public static class Template {

    }

    @Test
    public void test() throws Throwable {
        printClass(Template.class);
        CodeGen codegen = new CodeGen(Interface.class.getName(), ImmutableSet.of(Interface.class));
        codegen.constructor(ImmutableList.of(Interface.class));


        Interface<String> deleagte = new Interface<String>() {
            @Override
            public String print(String input) {
                return input;
            }
        };

        Arrays.stream(deleagte.getClass().getDeclaredMethods())
                .forEach((method) -> {
                   // codegen.adopt(method, );
                });

        // last check
        printClass(codegen.writer.toByteArray());
        Interface instance = Interface.class.cast(
                codegen.make().getConstructor( //
                        Interface.class
                ).newInstance(
                        deleagte
                ) //
        );
                /*
        Method method = Interface.class.getDeclaredMethod("print",Object.class);
        LOGGER.info(""+method);
        Arrays.stream(instance.getClass().getDeclaredFields()).forEach(System.out::println);
        */
    }
}

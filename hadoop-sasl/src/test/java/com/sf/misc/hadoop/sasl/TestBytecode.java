package com.sf.misc.hadoop.sasl;

import org.junit.Test;
import org.objectweb.asm.ClassReader;

import java.io.PrintWriter;

public class TestBytecode {
    public static class A {
    }

    public static class TemplateParent {
        TemplateParent(A a) {

        }
    }

    public static class Template extends TemplateParent {
        public Template() throws IllegalAccessException, InstantiationException {
            super(A.class.newInstance());
        }
    }

    protected void printClass(Class<?> clazz) throws Throwable {
        ClassReader reader = new ClassReader(clazz.getName());
        //reader.accept(new TraceClassVisitor(new PrintWriter(System.out)), 0);
    }

    @Test
    public void test() throws Throwable {
        printClass(Template.class);
    }
}

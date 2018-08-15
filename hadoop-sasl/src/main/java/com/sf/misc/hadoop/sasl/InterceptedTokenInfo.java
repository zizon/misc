package com.sf.misc.hadoop.sasl;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import jdk.internal.org.objectweb.asm.util.TraceClassVisitor;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenInfo;
import org.apache.hadoop.security.token.TokenSelector;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.io.PrintWriter;
import java.lang.annotation.Annotation;
import java.lang.invoke.MethodType;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;

public class InterceptedTokenInfo {

    protected static class Geneartor extends ClassLoader {
        public Geneartor(ClassLoader parent) {
            super(parent);
        }

        public Class<?> genearte(String className, byte[] bytecode) {
            printClass(bytecode);
            return defineClass(className, bytecode, 0, bytecode.length);
        }
    }

    protected static final ConcurrentMap<Class<?>, ListenableFuture<Class<?>>> DECORATED_SELECTOR = Maps.newConcurrentMap();
    protected static final ListeningExecutorService POOL = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
    protected static final TokenSelectorGenerator.Geneartor GENEARTOR = new TokenSelectorGenerator.Geneartor(Thread.currentThread().getContextClassLoader());

    public static TokenInfo make(TokenInfo provided) {
        return new TokenInfo() {
            @Override
            public Class<? extends Annotation> annotationType() {
                return provided.annotationType();
            }

            @Override
            public Class<? extends TokenSelector<? extends TokenIdentifier>> value() {
                return decorate(provided.value());
            }
        };
    }

    public static Class<? extends TokenSelector<? extends TokenIdentifier>> decorate(Class<? extends TokenSelector<? extends TokenIdentifier>> provided) {
        ListenableFuture<Class<?>> generated = DECORATED_SELECTOR.compute(provided, (key, value) -> {
            if (value == null) {
                value = generate(key);
            }

            return value;
        });

        return (Class<? extends TokenSelector<? extends TokenIdentifier>>) Futures.getUnchecked(generated);
    }

    protected static void printClass(byte[] clazz) {
        jdk.internal.org.objectweb.asm.ClassReader reader = new jdk.internal.org.objectweb.asm.ClassReader(clazz);
        reader.accept(new TraceClassVisitor(new PrintWriter(System.out)), 0);
    }

    protected static ListenableFuture<Class<?>> generate(Class<?> provided) {
        return POOL.submit(() -> doGeneate(provided));
    }

    protected static Class<?> doGeneate(Class<?> provided) {
        ClassWriter writer = new ClassWriter(ClassWriter.COMPUTE_FRAMES);
        Type this_type = typeName(Type.getType(provided), "Proxy");
        // declare class
        declareClass(writer, this_type);

        // constructor
        constructor(writer, provided);

        writer.visitEnd();
        return GENEARTOR.genearte(this_type.getClassName(), writer.toByteArray());
    }

    protected static void declareClass(ClassWriter writer, Type this_type) {
        writer.visit(
                Opcodes.V1_6,
                Opcodes.ACC_PUBLIC,
                this_type.getInternalName(),
                null,
                Type.getInternalName(InterceptedTokenSelector.class),
                new String[0]
        );
    }

    protected static void constructor(ClassWriter writer, Class<?> provided) {
        MethodVisitor method_writer = writer.visitMethod(
                Opcodes.ACC_PUBLIC,
                "<init>",
                MethodType.methodType(void.class).toMethodDescriptorString(),
                null,
                new String[0]
        );

        // push this
        method_writer.visitVarInsn(Opcodes.ALOAD, 0);

        // push class
        method_writer.visitLdcInsn(Type.getType(provided));

        // newInstance
        method_writer.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                Type.getInternalName(Class.class),
                "newInstance",
                MethodType.methodType(Object.class).toMethodDescriptorString()
        );

        method_writer.visitTypeInsn(Opcodes.CHECKCAST, Type.getInternalName(provided));

        // super
        method_writer.visitMethodInsn(
                Opcodes.INVOKESPECIAL,
                Type.getInternalName(InterceptedTokenSelector.class),
                "<init>",
                MethodType.methodType(void.class,
                        TokenSelector.class
                ).toMethodDescriptorString()
        );

        // end
        method_writer.visitMaxs(0, 0);
        method_writer.visitInsn(Opcodes.RETURN);
        method_writer.visitEnd();
    }

    protected static Type typeName(Type type, String postfix) {
        return Type.getType(type.getDescriptor().replace(";", postfix + ";"));
    }

}

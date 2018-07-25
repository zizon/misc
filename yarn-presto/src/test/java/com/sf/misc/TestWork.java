package com.sf.misc;

import com.sf.misc.classloaders.ClassResolver;
import com.sf.misc.yarn.rpc.ProtocolProxy;
import com.sf.misc.yarn.rpc.YarnNMProtocol;
import io.airlift.bytecode.DynamicClassLoader;
import org.apache.hadoop.security.GroupMappingServiceProvider;
import org.apache.hadoop.security.JniBasedUnixGroupsMappingWithFallback;
import org.junit.Assert;
import org.junit.Test;
import org.objectweb.asm.Attribute;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.util.TraceClassVisitor;

import java.io.PrintWriter;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestWork {

    protected Set<Class<?>> findInterfaces(Class<?> to_infer) {
        Stream<Class<?>> indrect = Arrays.stream(to_infer.getInterfaces()) //
                .flatMap((iface) -> {
                    return findInterfaces(iface).parallelStream();
                });
        return Stream.concat(Arrays.stream(to_infer.getInterfaces()), indrect).collect(Collectors.toSet());
    }

    @Test
    public void test() throws Exception {
        Assert.assertTrue(GroupMappingServiceProvider.class.isAssignableFrom(JniBasedUnixGroupsMappingWithFallback.class));

        Type type = Type.getObjectType(("generated." + YarnNMProtocol.class.getName()).replace(".", "/"));
        ClassWriter writer = new ClassWriter(ClassWriter.COMPUTE_FRAMES);

        writer.visit(
                Opcodes.V1_8,
                Opcodes.ACC_PUBLIC,
                type.getInternalName(),
                null,
                Type.getInternalName(Object.class),
                new String[]{Type.getInternalName(YarnNMProtocol.class)}
        );


        MethodVisitor method = writer.visitMethod(Opcodes.ACC_PUBLIC, "<init>", Type.getMethodDescriptor(Type.getType(void.class)), null, null);
        method.visitVarInsn(Opcodes.ALOAD, 0);
        method.visitMethodInsn(Opcodes.INVOKESPECIAL, Type.getInternalName(Object.class), "<init>", Type.getMethodDescriptor(Type.getType(void.class)), false);
        method.visitInsn(Opcodes.RETURN);
        method.visitMaxs(0, 0);
        method.visitEnd();


        writer.visitEnd();

        //Type.getType(class_name).getClassName()
        YarnNMProtocol craft = (YarnNMProtocol) new DynamicClassLoader(Thread.currentThread().getContextClassLoader()).defineClass(type.getClassName(), writer.toByteArray()).newInstance();

        craft.test();

        YarnNMProtocol proxy = new ProtocolProxy<YarnNMProtocol>(YarnNMProtocol.class, new Object[]{craft}).make();
        proxy.test();
    }

}

package com.sf.misc.bytecode;

import com.google.common.collect.Lists;
import io.airlift.bytecode.DynamicClassLoader;
import io.airlift.log.Logger;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Handle;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.util.TraceClassVisitor;

import java.io.PrintWriter;
import java.lang.invoke.CallSite;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class AnyCast {

    public static final Logger LOGGER = Logger.get(AnyCast.class);

    protected final List<Object> delegates;

    protected static class CodeGenContext {
        protected static final DynamicClassLoader CODEGEN_CLASS_LOADER = new DynamicClassLoader(Thread.currentThread().getContextClassLoader());

        protected final ClassWriter writer;
        protected final Class<?> protocol;
        protected final List<Object> delegates;

        protected final Type generated_type;

        protected CodeGenContext(Class<?> protocol, List<Object> delegates) {
            this.writer = new ClassWriter(org.objectweb.asm.ClassWriter.COMPUTE_FRAMES);
            this.protocol = protocol;
            this.delegates = delegates;

            String identify = ("" + delegates.parallelStream()
                    .map(Object::getClass) //
                    .map(Class::getName) //
                    .collect(Collectors.joining("")) //
                    .hashCode()).replace("-", "_");

            generated_type = Type.getType(Type.getDescriptor(protocol).replace(";", "") + "_" + identify);
        }

        protected Object newInstance() {
            try {
                return CODEGEN_CLASS_LOADER.defineClass(generated_type.getClassName(), writer.toByteArray()).newInstance();
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException(e);
            }
        }


        protected CodeGenContext printClass() {
            ClassReader reader = new ClassReader(writer.toByteArray());
            reader.accept(new TraceClassVisitor(new PrintWriter(System.out)), 0);
            return this;
        }

        protected Class<?> validate() {
            return CODEGEN_CLASS_LOADER.defineClass(generated_type.getClassName(), writer.toByteArray());
        }

    }

    public AnyCast() {
        this.delegates = Lists.newLinkedList();
    }

    public AnyCast adopt(Object object) {
        if (object != null) {
            this.delegates.add(object);
        }

        return this;
    }

    protected CodeGenContext codegen(Class<?> protocol) {
        CodeGenContext context = new CodeGenContext(protocol, delegates);

        // declare
        declareClass(context);

        // make constructor
        generateConstructor(context);

        // delegate method
        generateMethod(context);

        // rewrite dynamic callsite
        return context;
    }

    protected void declareClass(CodeGenContext context) {
        context.writer.visit(
                Opcodes.V1_8,
                Opcodes.ACC_PUBLIC,
                context.generated_type.getInternalName(),
                null,
                Type.getInternalName(Object.class),
                new String[]{
                        Type.getInternalName(context.protocol),
                        Type.getInternalName(DynamicCallSite.class)
                }
        );
        return;
    }

    protected void generateConstructor(CodeGenContext context) {
        ClassWriter writer = context.writer;
        Type filed_type = Type.getType(DynamicCallSite.class);
        Type field_owner_type = context.generated_type;

        // declare
        MethodVisitor method_writer = writer.visitMethod(
                Opcodes.ACC_PUBLIC,
                "<init>",
                MethodType.methodType(
                        void.class
                ).toMethodDescriptorString(),
                null,
                new String[0]
        );

        // call super
        method_writer.visitVarInsn(Opcodes.ALOAD, 0);
        method_writer.visitMethodInsn( //
                Opcodes.INVOKESPECIAL, //
                Type.getInternalName(Object.class), //
                "<init>", //
                MethodType.methodType(void.class) //
                        .toMethodDescriptorString(), //
                false //
        );

        // end
        method_writer.visitMaxs(0, 0);
        method_writer.visitInsn(org.objectweb.asm.Opcodes.RETURN);
        method_writer.visitEnd();
        return;
    }

    protected void generateMethod(CodeGenContext context) {
        ClassWriter writer = context.writer;

        // generate method
        Arrays.stream(context.protocol.getMethods())
                .filter((method) -> {
                    return (method.getModifiers() & Modifier.STATIC) == 0;
                })
                .forEach((method) -> {
                    Type method_type = Type.getType(method);

                    // declare
                    MethodVisitor method_writer = writer.visitMethod(
                            Opcodes.ACC_PUBLIC,  //
                            method.getName(),  //
                            method_type.getDescriptor(), //
                            null, //
                            Arrays.stream(method.getExceptionTypes()).parallel() //
                                    .map(Type::getInternalName) //
                                    .toArray(String[]::new) //
                    );

                    // push this
                    method_writer.visitVarInsn(Opcodes.ALOAD, 0);

                    // push parameters
                    Class<?>[] parameters = method.getParameterTypes();
                    IntStream.range(0, parameters.length).forEach((index) -> {
                        Class<?> parameter_class = parameters[index];
                        Type parameter_type = Type.getType(parameter_class);

                        method_writer.visitVarInsn(parameter_type.getOpcode(org.objectweb.asm.Opcodes.ILOAD), index + 1);
                    });

                    LOGGER.info("" + DynamicCallSite.INVOKE_DYNAMIC_METHOD_TYPE + "+" + DynamicCallSite.INVOKE_DYNAMIC_METHOD);
                    // invoke dynamic
                    if (true) {
                        method_writer.visitInvokeDynamicInsn(
                                method.getName(), //
                                method_type.getDescriptor(), //
                                new Handle(
                                        Opcodes.H_INVOKEVIRTUAL,
                                        context.generated_type.getInternalName(),
                                        //Type.getInternalName(DynamicCallSite.class),
                                        DynamicCallSite.INVOKE_DYNAMIC_METHOD,
                                        DynamicCallSite.INVOKE_DYNAMIC_METHOD_TYPE.toMethodDescriptorString(),
                                        false
                                )
                        );
                    }else{
                        method_writer.visitInvokeDynamicInsn(
                                method.getName(),
                                method_type.getDescriptor(),
                                new Handle(
                                        Opcodes.H_INVOKESTATIC,
                                        Type.getType(AnycastOld.class).getInternalName(),
                                        "bootstrapDynamic",
                                        MethodType.methodType(
                                                CallSite.class, //
                                                MethodHandles.Lookup.class, //
                                                String.class, //
                                                MethodType.class
                                        ).toMethodDescriptorString(), //
                                        AnycastOld.class.isInterface()
                                )//
                        );
                    }


                    method_writer.visitMaxs(0, 0);
                    method_writer.visitInsn(Type.getType(method.getReturnType()).getOpcode(Opcodes.IRETURN));
                    method_writer.visitEnd();
                });

        context.writer.visitEnd();
        return;
    }

    protected void rewriteDynamicCallsite(CodeGenContext context) {
        return;
    }

}

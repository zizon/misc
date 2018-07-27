package com.sf.misc.yarn.rpc;

import com.google.common.collect.Streams;
import com.sf.misc.async.Entrys;
import com.sun.tools.javap.CodeWriter;
import io.airlift.bytecode.DynamicClassLoader;
import io.airlift.log.Logger;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

public class CodeGen {

    public static final Logger LOGGER = Logger.get(CodeGen.class);

    protected static final DynamicClassLoader CODEGEN_CLASS_LOADER = new DynamicClassLoader(Thread.currentThread().getContextClassLoader());

    protected final ClassWriter writer;
    protected final Type type;

    public CodeGen(String classname, Set<Class<?>> interfaces) {
        this.type = Type.getObjectType(("generated." + classname).replace(".", "/"));
        this.writer = new ClassWriter(ClassWriter.COMPUTE_FRAMES);
        this.writer.visit(
                Opcodes.V1_8,
                Opcodes.ACC_PUBLIC,
                type.getInternalName(),
                null,
                Type.getInternalName(Object.class),
                interfaces.parallelStream() //
                        .map(Type::getInternalName) //
                        .collect(Collectors.toList()) //
                        .toArray(new String[0])
        );
    }

    public List<Map.Entry<Type, String>> constructor(List<Class<?>> constructor_arguments) {
        MethodVisitor method = writer.visitMethod( //
                Opcodes.ACC_PUBLIC, //
                "<init>", //
                Type.getMethodDescriptor( //
                        Type.getType(void.class), //
                        constructor_arguments.stream().sequential() //
                                .map((clazz) -> Type.getType(clazz)) //
                                .collect(Collectors.toList()) //
                                .toArray(new Type[0]) //
                ), //
                null, //
                null //
        );

        // call super
        method.visitVarInsn(Opcodes.ALOAD, 0);
        method.visitMethodInsn(Opcodes.INVOKESPECIAL, Type.getInternalName(Object.class), "<init>", Type.getMethodDescriptor(Type.getType(void.class)), false);

        // for field
        List<Map.Entry<Type, String>> fileld_names = IntStream.range(0, constructor_arguments.size()).sequential()
                .mapToObj((index) -> {
                    int var_index = index + 1;
                    Class<?> clazz = constructor_arguments.get(index);

                    String field_name = clazz.getName()
                            .replace(".", "_")
                            .replace(";", "")
                            .replace("[", "array_")
                            + "_" + var_index;
                    LOGGER.info(field_name);
                    Type argumetn_type = Type.getType(clazz);

                    // create field
                    writer.visitField(
                            Opcodes.ACC_PROTECTED | Opcodes.ACC_FINAL,
                            field_name,
                            argumetn_type.getDescriptor(),
                            null,
                            null
                    ).visitEnd();

                    // set in constructor
                    method.visitVarInsn(Opcodes.ALOAD, 0);
                    method.visitVarInsn(argumetn_type.getOpcode(Opcodes.ILOAD), var_index);
                    method.visitFieldInsn(Opcodes.PUTFIELD, type.getInternalName(), field_name, argumetn_type.getDescriptor());

                    return Entrys.<Type, String>newImmutableEntry(argumetn_type, field_name);
                })
                .collect(Collectors.toList());

        // end constructor
        method.visitMaxs(0, 0);
        method.visitInsn(Opcodes.RETURN);
        method.visitEnd();
        return fileld_names;
    }

    public void adopt(Method method, String field_name, Type field_type) {
        Type method_type = Type.getType(method);

        MethodVisitor method_writer = writer.visitMethod(
                method.getModifiers(), //
                method_type.getInternalName(), //
                method_type.getDescriptor(),
                null,
                Arrays.stream(method.getExceptionTypes()).parallel()
                        .map(Type::getDescriptor) //
                        .collect(Collectors.toList()) //
                        .toArray(new String[0]) //
        );

        // get field
        method_writer.visitVarInsn(Opcodes.ALOAD, 0);
        method_writer.visitVarInsn(
                field_type.getOpcode(Opcodes.ILOAD), //
                writer.newField( //
                        type.getInternalName(), //
                        field_name, //
                        field_type.getDescriptor() //
                ) //
        );
        method_writer.visitFieldInsn(
                Opcodes.GETFIELD, //
                type.getInternalName(), //
                field_name, //
                field_type.getDescriptor() //
        );

        // invoke
        method_writer.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                field_type.getInternalName(),
                method_type.getInternalName(),
                method_type.getDescriptor(),
                field_type.getClass().isInterface()
        );

        // return
        method_writer.visitMaxs(0, 0);
        method_writer.visitInsn(method_type.getReturnType().getOpcode(Opcodes.IRETURN));
        method_writer.visitEnd();

        return;
    }

    protected ClassWriter writer() {
        return writer;
    }

    public Class<?> make() {
        return CODEGEN_CLASS_LOADER.defineClass(type.getClassName(), writer.toByteArray());
    }
}

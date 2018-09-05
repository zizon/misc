package com.sf.misc.presto;

import com.facebook.presto.server.PrestoServer;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import com.sf.misc.async.ListenablePromise;
import com.sf.misc.async.Promises;
import com.sf.misc.bytecode.AnyCast;
import com.sf.misc.classloaders.ClassResolver;
import io.airlift.log.Logger;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.FieldInsnNode;
import org.objectweb.asm.tree.FrameNode;
import org.objectweb.asm.tree.LdcInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.util.TraceClassVisitor;

import java.io.InputStream;
import java.io.PrintWriter;
import java.lang.invoke.MethodType;
import java.util.Iterator;
import java.util.stream.Collectors;

public class PrestoServerBuilder {

    public static final Logger LOGGER = Logger.get(PrestoServerBuilder.class);

    public static abstract class PrestoServerRunner implements Runnable {
        protected abstract Iterable<Module> getAdditionalModules();
    }

    protected final ImmutableList.Builder<Module> modules;
    protected final ListenablePromise<Class<?>> clazz;
    protected final Type geneate_type;
    protected final boolean debug;

    public PrestoServerBuilder(boolean debug) {
        this.debug = debug;
        this.modules = ImmutableList.builder();
        this.geneate_type = Type.getType(Type.getDescriptor(PrestoServer.class).replace(";", "") + "Hacked;");
        clazz = creatClass();
    }


    public PrestoServerBuilder() {
        this(false);
    }

    public PrestoServerBuilder add(Module module) {
        this.modules.add(module);
        return this;
    }

    public ListenablePromise<PrestoServerRunner> build() {
        return clazz.transform((clazz) -> {
            return PrestoServerRunner.class.cast( //
                    clazz.getConstructor( //
                            Iterable.class, //
                            SqlParserOptions.class //
                    ).newInstance( //
                            this.modules.build(), //
                            new SqlParserOptions() //
                    ) //
            );
        });
    }

    protected ListenablePromise<ClassNode> copyPrestoServerClassDefination() {
        return Promises.submit(() -> {
            try (InputStream stream = ClassResolver.locate(PrestoServer.class).get().openConnection().getInputStream()) {
                return new ClassReader(stream);
            }
        }).transform((reader) -> {
            // copy class
            ClassNode copyed = new ClassNode();
            reader.accept(copyed, 0);

            return copyed;
        });
    }

    protected ClassNode rewriteOwner(ClassNode class_node) {
        class_node.methods.parallelStream()
                .forEach((method) -> {
                    method.instructions.iterator()
                            .forEachRemaining((instructions) -> {
                                // skip not method
                                if (instructions instanceof MethodInsnNode) {
                                    // clearify type
                                    MethodInsnNode method_node = (MethodInsnNode) instructions;

                                    // fix name
                                    if (Type.getInternalName(PrestoServer.class).equals(method_node.owner)) {
                                        method_node.owner = geneate_type.getInternalName();
                                    }

                                    return;
                                } else if (instructions instanceof FieldInsnNode) {
                                    FieldInsnNode field_node = (FieldInsnNode) instructions;

                                    if (Type.getInternalName(PrestoServer.class).equals(field_node.owner)) {
                                        field_node.owner = geneate_type.getInternalName();
                                    }

                                    return;
                                } else if (instructions instanceof LdcInsnNode) {
                                    LdcInsnNode ldc_node = (LdcInsnNode) instructions;
                                    if (ldc_node.cst instanceof Type) {
                                        Type ldc_type = (Type) ldc_node.cst;
                                        if (Type.getType(PrestoServer.class).equals(ldc_type)) {
                                            ldc_node.cst = geneate_type;
                                        }
                                    }

                                    return;
                                } else if (instructions instanceof FrameNode) {
                                    FrameNode frame_node = (FrameNode) instructions;
                                    if (frame_node.local == null) {
                                        return;
                                    }


                                    frame_node.local = frame_node.local.parallelStream().map((type) -> {
                                        if (method.name.equals("run")) {
                                            LOGGER.info("method:" + method + " locals type:" + type + " type:" + type.getClass());
                                        }

                                        if (Type.getType(PrestoServer.class).getInternalName().equals(type)) {
                                            return geneate_type.getInternalName();
                                        }

                                        return type;
                                    }).collect(Collectors.toList());

                                    return;
                                }
                            });

                    // local variable
                    method.localVariables.parallelStream()
                            .forEach((local_variable) -> {
                                if (local_variable.name.equals("this") && local_variable.desc.equals(Type.getDescriptor(PrestoServer.class))) {
                                    local_variable.desc = geneate_type.getDescriptor();
                                }
                                //LOGGER.info("local variable:" + local_variable);
                            });
                });
        return class_node;
    }

    protected ClassNode removeMethods(ClassNode class_node) {
        class_node.methods = class_node.methods.parallelStream()
                .filter((method) -> {
                    switch (method.name) {
                        case "main":
                        case "getAdditionalModules":
                        case "<init>":
                            return false;
                        default:
                            return true;
                    }
                })
                .collect(Collectors.toList());
        return class_node;
    }

    protected ClassNode removePrestoSystemRequirements(ClassNode class_node) {
        class_node.methods.parallelStream()
                // find run method
                .filter((method) -> method.name.equals("run")
                        && method.desc.equals(MethodType.methodType(void.class).toMethodDescriptorString()))
                .forEach((method) -> {
                    Iterator<AbstractInsnNode> iterator = method.instructions.iterator();

                    // find candaite
                    while (iterator.hasNext()) {
                        AbstractInsnNode instuction = iterator.next();
                        if (instuction instanceof MethodInsnNode) {
                            MethodInsnNode method_instruction = (MethodInsnNode) instuction;

                            // remove methods
                            switch (method_instruction.name) {
                                case "verifyJvmRequirements":
                                case "verifySystemTimeIsReasonable":
                                    iterator.remove();
                                    break;
                            }
                        }
                    }
                });

        return class_node;
    }

    protected ListenablePromise<Class<?>> creatClass() {
        return copyPrestoServerClassDefination() //
                .transform((clazz) -> removeMethods(clazz))
                .transform((clazz) -> rewriteOwner(clazz))
                .transform((clazz) -> removePrestoSystemRequirements(clazz))
                .transform((clazz) -> {
                    ClassWriter writer = new ClassWriter(ClassWriter.COMPUTE_MAXS);
                    clazz.accept(writer);
                    return writer;
                })
                .transform(this::generateClass);
    }

    protected Class<?> generateClass(ClassWriter writer) {
        String module_field_name = "_modules_";

        // rewrite name
        writer.visit(
                Opcodes.V1_8,
                Opcodes.ACC_PUBLIC,
                geneate_type.getInternalName(),
                null,
                Type.getInternalName(PrestoServerRunner.class),
                new String[0]
        );

        // new field
        writer.visitField(
                Opcodes.ACC_PROTECTED,
                module_field_name,
                Type.getDescriptor(Iterable.class),
                null,
                null
        );

        // generate <init>
        generateConstructor(writer, geneate_type, module_field_name);

        // generate getAdditionalModules
        geenrateGetAdditionalModules(writer, geneate_type, module_field_name);

        byte[] codes = writer.toByteArray();
        debug(codes);

        return AnyCast.codegenClassloader()
                .defineClass(geneate_type.getClassName(), codes);
    }

    protected void generateConstructor(ClassWriter writer, Type geneate_type, String module_field_name) {
        // constructor
        MethodVisitor constructor = writer.visitMethod(
                Opcodes.ACC_PUBLIC,
                "<init>",
                MethodType.methodType( //
                        void.class, //
                        Iterable.class,//
                        SqlParserOptions.class
                ).toMethodDescriptorString(),
                null,
                new String[0]
        );

        // call this
        // this()
        constructor.visitVarInsn(Opcodes.ALOAD, 0);
        constructor.visitMethodInsn( //
                Opcodes.INVOKESPECIAL, //
                Type.getInternalName(PrestoServerRunner.class), //
                "<init>", //
                MethodType.methodType(void.class) //
                        .toMethodDescriptorString(), //
                false //
        );

        // this alrady in stack.
        // just push paramaeter
        constructor.visitVarInsn(Opcodes.ALOAD, 0);
        constructor.visitVarInsn( //
                Type.getType(Iterable.class) //
                        .getOpcode(Opcodes.ILOAD), //
                1 //
        );
        constructor.visitFieldInsn(
                Opcodes.PUTFIELD,
                geneate_type.getInternalName(),
                module_field_name,
                Type.getDescriptor(Iterable.class)
        );

        // set sql parse option
        //constructor.visitVarInsn(Opcodes.ALOAD, 0);
        constructor.visitVarInsn(Opcodes.ALOAD, 0);
        constructor.visitVarInsn(
                Type.getType(SqlParserOptions.class)
                        .getOpcode(Opcodes.ILOAD),
                2
        );
        constructor.visitFieldInsn(
                Opcodes.PUTFIELD,
                geneate_type.getInternalName(),
                "sqlParserOptions",
                Type.getDescriptor(SqlParserOptions.class)
        );

        constructor.visitMaxs(0, 0);
        constructor.visitInsn(Opcodes.RETURN);
        constructor.visitEnd();
    }

    protected void geenrateGetAdditionalModules(ClassWriter writer, Type geneate_type, String module_field_name) {
        // declare
        MethodVisitor method_writer = writer.visitMethod(
                Opcodes.ACC_PROTECTED,
                "getAdditionalModules",
                MethodType.methodType(Iterable.class).toMethodDescriptorString(),
                null,
                new String[0]
        );

        // load this
        method_writer.visitVarInsn(Opcodes.ALOAD, 0);

        // load _module_
        method_writer.visitFieldInsn(
                Opcodes.GETFIELD,
                geneate_type.getInternalName(),
                module_field_name,
                Type.getDescriptor(Iterable.class)
        );

        // return
        method_writer.visitMaxs(0, 0);
        method_writer.visitInsn(Type.getType(Iterable.class).getOpcode(Opcodes.IRETURN));
        method_writer.visitEnd();
    }

    protected void debug(byte[] codes) {
        if (debug) {
            ClassReader reader = new ClassReader(codes);
            reader.accept(new TraceClassVisitor(new PrintWriter(System.out)), 0);
        }
    }
}

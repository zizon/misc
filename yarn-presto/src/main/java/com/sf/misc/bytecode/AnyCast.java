package com.sf.misc.bytecode;

import com.google.common.base.Predicates;
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
import java.lang.invoke.ConstantCallSite;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class AnyCast {

    public static final Logger LOGGER = Logger.get(AnyCast.class);

    protected static final String DELEGATE_FIELD_NAME = "delegate";
    protected static final Class<?> DELEGATE_FIELD_TYPE = List.class;

    protected static final String THIS_FIELD_NAME = "_this_";
    protected static final Class<?> THIS_FIELD_TYPE = Object.class;

    protected static final String INVOKE_DYNAMIC_METHOD = "callsite";
    protected static final MethodType INVOKE_DYNAMIC_METHOD_TYPE = MethodType.methodType(CallSite.class, MethodHandles.Lookup.class, String.class, MethodType.class);

    protected static final DynamicClassLoader DEFAULT_CODEGEN_CLASS_LOADER = new DynamicClassLoader(Thread.currentThread().getContextClassLoader());

    protected class CodeGenContext {
        protected final ClassWriter writer;
        protected final Class<?> protocol;
        protected final List<Object> delegates;

        protected final Type generated_type;
        protected final TypeCheck typecheck;

        protected CodeGenContext(Class<?> protocol, List<Object> delegates) {
            this.writer = new ClassWriter(ClassWriter.COMPUTE_FRAMES);
            this.protocol = protocol;
            this.delegates = delegates;
            this.typecheck = new TypeCheck();

            String identify = ("" + delegates.parallelStream()
                    .map(Object::toString) //
                    .collect(Collectors.joining("")) //
                    .hashCode() //
            ).replace("-", "_");

            generated_type = Type.getType(Type.getDescriptor(protocol).replace(";", "") + "_" + identify);
        }

        protected Object newInstance() {
            try {
                // create instance
                return codegenClassloader() //
                        .defineClass( //
                                generated_type.getClassName(), //
                                writer.toByteArray() //
                        ) //
                        .getConstructor(List.class) //
                        .newInstance(delegates);
            } catch (Throwable throwable) {
                throw new RuntimeException(throwable);
            }
        }

        protected CodeGenContext printClass() {
            ClassReader reader = new ClassReader(writer.toByteArray());
            reader.accept(new TraceClassVisitor(new PrintWriter(System.out)), 0);
            return this;
        }

        protected Class<?> validate() {
            return codegenClassloader().defineClass(generated_type.getClassName(), writer.toByteArray());
        }
    }

    public static CallSite callsite(MethodHandles.Lookup caller, String name, MethodType type) {
        try {
            return callsiteExceptional(caller, name, type);
        } catch (Throwable throwable) {
            throw new LinkageError("no binding found for:" + caller.lookupClass() + " of method:" + name + " type:" + type, throwable);
        }
    }

    protected static CallSite callsiteExceptional(MethodHandles.Lookup caller, String name, MethodType type) throws Throwable {
        // resolve this object
        Object this_object = caller.findStaticGetter(
                caller.lookupClass(), //
                AnyCast.THIS_FIELD_NAME, //
                AnyCast.THIS_FIELD_TYPE //
        ).invoke();

        // resolve this class
        Class<?> this_class = caller.lookupClass();

        // delegates
        List<Object> delegates = List.class.cast(
                caller.findStaticGetter(
                        this_class, //
                        AnyCast.DELEGATE_FIELD_NAME, //
                        AnyCast.DELEGATE_FIELD_TYPE //
                ).invoke()
        );

        TypeCheck typecheck = new TypeCheck();

        Optional<MethodHandle> candidate = delegates.parallelStream()
                .map((delegate) -> {
                    Class<?> found = typecheck.findAccesiableClass(delegate.getClass(), name, type);
                    if (found == null) {
                        return null;
                    }

                    try {
                        return MethodHandles.lookup().findVirtual(found, name, type).bindTo(delegate);
                    } catch (ReflectiveOperationException e) {
                        throw new RuntimeException("should not here", e);
                    }
                }) //
                .filter(Predicates.notNull()) //
                .findFirst();

        if (candidate.isPresent()) {
            return new ConstantCallSite(candidate.get().asType(type));
        }

        throw new NoSuchMethodException("no accesiable maching method handler:" + name + " type:" + type);
    }

    protected final List<Object> delegates;

    public AnyCast() {
        this.delegates = Lists.newLinkedList();
    }

    public AnyCast adopt(Object object) {
        if (object != null) {
            this.delegates.add(object);
        }

        return this;
    }

    public <T> T newInstance(Class<T> protocol) {
        return protocol.cast(codegen(protocol).newInstance());
    }

    protected CodeGenContext codegen(Class<?> protocol) {
        CodeGenContext context = new CodeGenContext(protocol, delegates);

        // declare
        declareClass(context);

        // declare static field
        declearDelegateField(context);

        // declare this field
        declareThisField(context);

        // make constructor
        generateConstructor(context);

        // delegate method
        generateMethod(context);

        return context;
    }

    protected void declareClass(CodeGenContext context) {
        // public class $protocol_gen_uniq_id implemnts DynamicCallSite,$protocol...{}
        context.writer.visit(
                Opcodes.V1_8,
                Opcodes.ACC_PUBLIC,
                context.generated_type.getInternalName(),
                null,
                Type.getInternalName(Object.class),
                new String[]{
                        Type.getInternalName(context.protocol)
                }
        );
        return;
    }

    protected void declearDelegateField(CodeGenContext context) {
        // public static List<Object> delegates;
        context.writer.visitField(
                Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC,
                DELEGATE_FIELD_NAME,
                Type.getDescriptor(DELEGATE_FIELD_TYPE),
                null,
                null
        ).visitEnd();
    }

    protected void declareThisField(CodeGenContext context) {
        // public static Object _this_;
        context.writer.visitField(
                Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC,
                THIS_FIELD_NAME,
                Type.getDescriptor(THIS_FIELD_TYPE),
                null,
                null
        ).visitEnd();
    }

    protected void generateConstructor(CodeGenContext context) {
        ClassWriter writer = context.writer;
        Type field_owner_type = context.generated_type;

        // declare
        // public $protocol(){}
        MethodVisitor method_writer = writer.visitMethod(
                Opcodes.ACC_PUBLIC,
                "<init>",
                MethodType.methodType(
                        void.class,
                        List.class
                ).toMethodDescriptorString(),
                null,
                new String[0]
        );

        // call super
        // super()
        method_writer.visitVarInsn(Opcodes.ALOAD, 0);
        method_writer.visitMethodInsn( //
                Opcodes.INVOKESPECIAL, //
                Type.getInternalName(Object.class), //
                "<init>", //
                MethodType.methodType(void.class) //
                        .toMethodDescriptorString(), //
                false //
        );

        // save this
        method_writer.visitVarInsn(Opcodes.ALOAD, 0);
        method_writer.visitFieldInsn(
                Opcodes.PUTSTATIC,
                context.generated_type.getInternalName(),
                THIS_FIELD_NAME,
                Type.getDescriptor(THIS_FIELD_TYPE)
        );

        // save delegate
        method_writer.visitVarInsn(Opcodes.ALOAD, 1);
        method_writer.visitFieldInsn(
                Opcodes.PUTSTATIC,
                context.generated_type.getInternalName(),
                DELEGATE_FIELD_NAME,
                Type.getDescriptor(DELEGATE_FIELD_TYPE)
        );

        // end
        method_writer.visitMaxs(0, 0);
        method_writer.visitInsn(Opcodes.RETURN);
        method_writer.visitEnd();
        return;
    }

    protected void generateMethod(CodeGenContext context) {
        ClassWriter writer = context.writer;

        // generate method
        Arrays.stream(context.protocol.getMethods())
                .filter((method) -> {
                    // filter static mathod
                    return (method.getModifiers() & Modifier.STATIC) == 0;
                })
                .filter((method) -> {
                    TypeCheck typecheck = context.typecheck;

                    // filter no delegate methods
                    return delegates.parallelStream()
                            .filter((delegate) -> {
                                return typecheck.findAccesiableClass(delegate.getClass(), method.getName(), typecheck.toType(method)) != null;
                            }) //
                            .findAny() //
                            .isPresent();
                })
                .forEach((method) -> {
                    Type method_type = Type.getType(method);

                    // declare
                    // public rtyp method(args) ...
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

                        method_writer.visitVarInsn( //
                                parameter_type.getOpcode(Opcodes.ILOAD), //
                                index + 1 //
                        );
                    });

                    // invoke dynamic
                    // bind methond handler in callsite
                    method_writer.visitInvokeDynamicInsn(
                            method.getName(), //
                            method_type.getDescriptor(), //
                            new Handle(
                                    Opcodes.H_INVOKESTATIC,
                                    Type.getInternalName(AnyCast.class),
                                    INVOKE_DYNAMIC_METHOD,
                                    INVOKE_DYNAMIC_METHOD_TYPE.toMethodDescriptorString(),
                                    AnyCast.class.isInterface()
                            )
                    );

                    method_writer.visitMaxs(0, 0);
                    method_writer.visitInsn(Type.getType(method.getReturnType()).getOpcode(Opcodes.IRETURN));
                    method_writer.visitEnd();
                });

        context.writer.visitEnd();
        return;
    }

    public static DynamicClassLoader codegenClassloader() {
        return DEFAULT_CODEGEN_CLASS_LOADER;
    }
}

package com.sf.misc.bytecode;

import com.google.common.base.Predicates;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import io.airlift.bytecode.DynamicClassLoader;
import io.airlift.log.Logger;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Handle;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.signature.SignatureWriter;
import sun.invoke.util.BytecodeDescriptor;

import java.lang.invoke.CallSite;
import java.lang.invoke.ConstantCallSite;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Anycast {

    public static final Logger LOGGER = Logger.get(Anycast.class);

    protected static final DynamicClassLoader CODEGEN_CLASS_LOADER = new DynamicClassLoader(Thread.currentThread().getContextClassLoader());

    protected static class MethodInfoBundle {
        protected final Object instance;
        protected final Method method;
        protected final Method protocol;
        protected final String field_name;

        protected MethodInfoBundle(Method method, Object object) {
            this(method, object, null);
        }

        protected MethodInfoBundle(Method method, Object object, Method protocol) {
            this.instance = object;
            this.method = method;
            this.protocol = protocol;
            this.field_name = makeFieldName(object);
        }

        protected static String makeFieldName(Object object) {
            return object.getClass().getName()
                    .replace(".", "_")
                    .replace(";", "")
                    .replace("[", "array_")
                    + "_" + object.hashCode();
        }
    }

    protected static class GeneratedInfoBundle {

        protected final Class<?> protocol;
        protected final Type type;
        protected final String name;
        protected final String internal;

        protected GeneratedInfoBundle(Class<?> protocol) {
            final String prefix = "generated";
            this.protocol = protocol;
            this.type = Type.getType(protocol);
            this.name = prefix + "." + protocol.getName();
            this.internal = prefix + "/" + type.getInternalName();
        }
    }

    protected final GeneratedInfoBundle target;
    protected final ConcurrentMap<String, Queue<MethodInfoBundle>> method_lookup;

    public Anycast(Class<?> target) {
        if (!target.isInterface()) {
            throw new IllegalArgumentException("target:" + target + " should be interface");
        }

        this.target = new GeneratedInfoBundle(target);
        this.method_lookup = Maps.newConcurrentMap();
    }

    public Anycast adopt(Object object) {
        // build lookup
        this.registerLookup(object);
        return this;
    }

    protected byte[] bytecode(MethodHandle callsite) {
        // find correct method for interface methods
        List<MethodInfoBundle> bundles = Arrays.stream(target.protocol.getMethods()).parallel()
                .map((method) -> {
                    // find cancidates
                    MethodInfoBundle matched = findBundle(method);

                    if (matched != null) {
                        return new MethodInfoBundle(matched.method, matched.instance, method);
                    }
                    return null;
                })
                .filter(Predicates.notNull())
                .collect(Collectors.toList());

        // new class writer
        ClassWriter writer = newWriter();

        // generate class
        writer = generateConstructor(writer, bundles);
        writer = generateMethod(writer, bundles, callsite);

        return writer.toByteArray();
    }

    protected ClassWriter newWriter() {
        ClassWriter writer = new ClassWriter(org.objectweb.asm.ClassWriter.COMPUTE_FRAMES);
        writer.visit(
                Opcodes.V1_8,
                Opcodes.ACC_PUBLIC,
                target.internal,
                null,
                Type.getInternalName(Object.class),
                new String[]{
                        Type.getType(target.protocol).getInternalName()
                }
        );
        return writer;
    }

    protected ClassWriter generateConstructor(ClassWriter writer, List<MethodInfoBundle> parameters) {
        List<Object> instances = parameters.parallelStream().map((bundle) -> bundle.instance)
                .distinct()
                .sorted((left, right) -> left.getClass().getName().compareTo(right.getClass().getName()))
                .collect(Collectors.toList());

        // declare
        MethodVisitor method = writer.visitMethod( //
                Opcodes.ACC_PUBLIC, //
                "<init>", //
                MethodType.methodType( //
                        void.class,  //
                        instances.parallelStream() //
                                .map(Object::getClass) //
                                .collect(Collectors.toList()) //
                ).toMethodDescriptorString(), //
                null, //
                null //
        );

        // call super
        method.visitVarInsn(Opcodes.ALOAD, 0);
        method.visitMethodInsn(Opcodes.INVOKESPECIAL, Type.getInternalName(Object.class), "<init>", MethodType.methodType(void.class).toMethodDescriptorString(), false);

        // set fields
        IntStream.range(0, instances.size()).forEach((order) -> {
            Object instance = instances.get(order);
            Type instance_type = Type.getType(instance.getClass());
            String field_name = MethodInfoBundle.makeFieldName(instance);

            // create field
            writer.visitField(
                    Opcodes.ACC_PROTECTED | Opcodes.ACC_FINAL,
                    MethodInfoBundle.makeFieldName(instance),
                    instance_type.getDescriptor(),
                    null,
                    null
            ).visitEnd();

            // set in constructor
            method.visitVarInsn(Opcodes.ALOAD, 0);
            method.visitVarInsn(instance_type.getOpcode(Opcodes.ILOAD), order + 1);
            method.visitFieldInsn(Opcodes.PUTFIELD, target.internal, field_name, instance_type.getDescriptor());
        });

        // end constructor
        method.visitMaxs(0, 0);
        method.visitInsn(Opcodes.RETURN);
        method.visitEnd();
        return writer;
    }

    protected ClassWriter generateMethod(ClassWriter writer, List<MethodInfoBundle> method_infos, MethodHandle callsite) {
        method_infos.forEach((method_info) -> {
            Method protocol = method_info.protocol;
            Type protocol_type = Type.getType(method_info.protocol);

            // declare
            MethodVisitor method_writer = writer.visitMethod(
                    protocol.getModifiers() & (~Modifier.ABSTRACT),
                    protocol.getName(),
                    protocol_type.getDescriptor(),
                    null,
                    Arrays.stream(protocol.getExceptionTypes()).parallel()
                            .map((clazz) -> Type.getType(clazz).getInternalName())
                            .toArray(String[]::new)
            );

            // load field
            method_writer.visitVarInsn(Opcodes.ALOAD, 0);
            method_writer.visitFieldInsn(Opcodes.GETFIELD, target.internal, method_info.field_name, Type.getDescriptor(method_info.instance.getClass()));
            //method_writer.visitTypeInsn();

            // push parameters
            Class<?>[] parameters = protocol.getParameterTypes();
            IntStream.range(0, parameters.length).forEach((index) -> {
                Class<?> parameter_class = parameters[index];
                Type parameter_type = Type.getType(parameter_class);

                method_writer.visitVarInsn(parameter_type.getOpcode(Opcodes.ILOAD), index + 1);
            });

            if (callsite != null) {
                // invoke dynamic
                method_writer.visitInvokeDynamicInsn(
                        protocol.getName(), //
                        protocol_type.getDescriptor(), //
                        new Handle(
                                Opcodes.H_INVOKESTATIC,
                                Type.getType(Anycast.class).getInternalName(),
                                "bootstrapDynamic",
                                MethodType.methodType(
                                        CallSite.class, //
                                        MethodHandles.Lookup.class, //
                                        String.class, //
                                        MethodType.class, //
                                        String.class
                                ).toMethodDescriptorString(), //
                                Anycast.class.isInterface()
                        ),//
                        "hello"
                );
            } else {
                method_writer.visitMethodInsn(
                        Opcodes.INVOKEVIRTUAL,
                        Type.getInternalName(method_info.instance.getClass()),
                        method_info.protocol.getName(),
                        protocol_type.getDescriptor(),
                        false
                );
            }

            // end constructor
            method_writer.visitMaxs(0, 0);
            method_writer.visitInsn(protocol_type.getReturnType().getOpcode(Opcodes.IRETURN));
            method_writer.visitEnd();
        });

        return writer;
    }

    private static void printArgs(Object... args) {
        LOGGER.info("args:" + java.util.Arrays.deepToString(args));
    }

    private static MethodHandle printArgs;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            Class thisClass = lookup.lookupClass();  // (who am I?)
            printArgs = lookup.findStatic(thisClass,
                    "printArgs", MethodType.methodType(void.class, Object[].class));
        } catch (Throwable e) {
            LOGGER.error(e, "fail to bind printArgs");
        }
    }

    public static CallSite bootstrapDynamic(MethodHandles.Lookup caller, String name, MethodType type, String passed) {
        // ignore caller and name, but match the type:
        LOGGER.info("name:" + name + " type:" + type + " passed:" + passed);
        return new ConstantCallSite(printArgs.asType(type));
    }

    protected MethodInfoBundle findBundle(Method method) {
        // find cancidates
        Queue<MethodInfoBundle> methods = method_lookup.get(method.getName());
        if (methods == null) {
            LOGGER.warn("no method bouding candidate found for:" + method);
            return null;
        }

        // find match method
        List<MethodInfoBundle> matched = methods.parallelStream()
                .filter((candiate) -> {
                    Method from = candiate.method;
                    // parameter check
                    if (from.getParameterCount() != method.getParameterCount()) {
                        return false;
                    }

                    // type compatible?
                    Class<?>[] from_paramerters = from.getParameterTypes();
                    Class<?>[] parameter = method.getParameterTypes();
                    return IntStream.range(0, method.getParameterCount()).parallel()
                            .mapToObj((index) -> {
                                return from_paramerters[index].isAssignableFrom(parameter[index]);
                            })
                            .reduce(Boolean::logicalAnd)
                            .orElse(true);
                }) //
                .filter((candiate) -> {
                    // return check
                    Method from = candiate.method;
                    return from.getReturnType().isAssignableFrom(method.getReturnType());
                })
                .filter((candiate) -> {
                    // exception check
                    Method from = candiate.method;
                    return Arrays.stream(from.getExceptionTypes()).parallel()
                            .map((from_exception_type) -> {
                                return Arrays.stream(method.getExceptionTypes()).parallel()
                                        .map((provided_exception_type) -> {
                                            // provided interface exception is sub class of candidate exception?
                                            return provided_exception_type.isAssignableFrom(from_exception_type);
                                        })//
                                        .reduce(Boolean::logicalOr) //
                                        .orElse(true);
                            })
                            .reduce(Boolean::logicalAnd) // all expection should convertable
                            .orElse(true);
                })
                .collect(Collectors.toList());

        // check match
        if (matched.isEmpty()) {
            LOGGER.warn("no maching method bind to:" + method);
            return null;
        } else if (matched.size() != 1) {
            throw new IllegalStateException("multiple binding for method:" + method + " candidates:"
                    + matched.stream() //
                    .map(Object::toString) //
                    .collect(Collectors.joining(",")));
        }

        return matched.get(0);
    }

    protected void registerLookup(Object object) {
        //make method lookup
        Arrays.stream(object.getClass().getMethods()).parallel()
                .forEach((method) -> {
                    method_lookup.compute(method.getName(), (name, collection) -> {
                        if (collection == null) {
                            collection = Queues.newConcurrentLinkedQueue();
                        }

                        method.setAccessible(true);
                        collection.offer(new MethodInfoBundle(method, object));
                        return collection;
                    });
                });
    }
}

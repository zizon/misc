package com.sf.misc.bytecode;

import com.google.common.base.Predicates;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.sf.misc.async.Entrys;
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
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class AnycastOld {

    public static final Logger LOGGER = Logger.get(AnycastOld.class);

    protected static final DynamicClassLoader CODEGEN_CLASS_LOADER = new DynamicClassLoader(Thread.currentThread().getContextClassLoader());

    protected static class MethodInfoBundle {
        protected final Object instance;
        protected final Method method;
        protected final Method protocol;
        protected final String field_name;
        //protected final Class<?> owner;

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

        public Class<?> instanceType() {
            return instanceType(instance);
        }

        protected static Class<?> instanceType(Object instance) {
            if (instance.getClass().getEnclosingClass() != null) {
                return Object.class;
            } else {
                return instance.getClass();
            }
        }

    }

    protected static class GeneratedInfoBundle {

        protected final Class<?> protocol;
        protected final String name;
        protected final String internal;
        protected final byte[] bytecode;
        protected final List<Object> parameters;

        protected GeneratedInfoBundle(Class<?> protocol, ConcurrentMap<String, Queue<MethodInfoBundle>> lookups) {
            this(protocol, lookups, null);
        }

        protected GeneratedInfoBundle(Class<?> protocol, ConcurrentMap<String, Queue<MethodInfoBundle>> lookups, byte[] bytecode) {
            final String prefix = "generated";
            this.protocol = protocol;

            String posfix = ("_" + lookups.values().parallelStream() //
                    .flatMap(Queue::parallelStream) //
                    .map((method_bundle) -> method_bundle.instance.getClass().getName()) //
                    .distinct() //
                    .sorted() //
                    .collect(Collectors.joining()) //
                    .hashCode()).replace("-", "_");

            this.name = prefix + "." + protocol.getName() + posfix;
            this.internal = prefix + "/" + Type.getInternalName(protocol) + posfix;
            this.bytecode = bytecode;
            this.parameters = null;
        }

        protected GeneratedInfoBundle(Class<?> protocol, String name, String internal, byte[] bytecode, List<Object> parameters) {
            this.protocol = protocol;
            this.name = name;
            this.internal = internal;
            this.bytecode = bytecode;
            this.parameters = null;
        }

        public GeneratedInfoBundle withBytecode(byte[] bytecode) {
            return new GeneratedInfoBundle(protocol, name, internal, bytecode, null);
        }

        public GeneratedInfoBundle withParameter(List<Object> parameter) {
            return new GeneratedInfoBundle(protocol, name, internal, bytecode, parameter);
        }
    }

    //protected final GeneratedInfoBundle target;
    protected final ConcurrentMap<String, Queue<MethodInfoBundle>> method_lookup;

    public AnycastOld() {
        this.method_lookup = Maps.newConcurrentMap();
    }

    public AnycastOld adopt(Object object) {
        // build lookup
        this.registerLookup(object);
        return this;
    }

    public <T> T newInstance(Class<T> instance, MethodHandle callsite) {
        return instance.cast(newInstance(bytecode(instance, callsite)));
    }

    protected Object newInstance(GeneratedInfoBundle generate_info) {
        Class<?> generated = CODEGEN_CLASS_LOADER.defineClass(generate_info.name, generate_info.bytecode);
        try {
            return generated.getConstructor( //
                    generate_info.parameters.parallelStream() //
                            .map(Object::getClass) //
                            .toArray(Class[]::new) //
            ).newInstance(
                    generate_info.parameters.toArray()
            );

        } catch (NoSuchMethodException e) {
            throw new RuntimeException("fail to genreate anycast for class:" + generate_info.protocol, e);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("fail to instantiate anycast for class:" + generate_info.protocol, e);
        }
    }

    protected GeneratedInfoBundle generate(GeneratedInfoBundle generate_info, List<MethodInfoBundle> method_deletates, MethodHandle callsite) {

        // new class writer
        ClassWriter writer = newWriter(generate_info);

        // generate constructor
        List<Object> parameters = generateConstructor(writer, method_deletates, generate_info);

        // genearte methods
        generateMethod(writer, method_deletates, callsite, generate_info);

        return generate_info.withBytecode(writer.toByteArray()) //
                .withParameter(parameters);
    }

    protected GeneratedInfoBundle bytecode(Class<?> clazz, MethodHandle callsite) {
        GeneratedInfoBundle target = new GeneratedInfoBundle(clazz, method_lookup);
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

        return generate(target, bundles, callsite);
    }

    protected ClassWriter newWriter(GeneratedInfoBundle target) {
        ClassWriter writer = new ClassWriter(org.objectweb.asm.ClassWriter.COMPUTE_FRAMES);
        Class<?> protocol = target.protocol;
        if (protocol.isInterface()) {
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
        } else {
            writer.visit(
                    Opcodes.V1_8,
                    Opcodes.ACC_PUBLIC,
                    target.internal,
                    null,
                    Type.getInternalName(protocol),
                    new String[0]
            );
        }

        return writer;
    }

    protected List<Object> generateConstructor(ClassWriter writer, List<MethodInfoBundle> parameters, GeneratedInfoBundle target) {
        List<Object> instances = parameters.parallelStream().map((bundle) -> bundle.instance)
                .distinct() //
                .sorted((left, right) -> left.getClass().getName().compareTo(right.getClass().getName())) //
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
            Type instance_type = Type.getType(MethodInfoBundle.instanceType(instance));
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
        return instances;
    }

    protected void generateMethod(ClassWriter writer, List<MethodInfoBundle> method_infos, MethodHandle callsite, GeneratedInfoBundle target) {
        method_infos.forEach((method_info) -> {
            Method protocol = method_info.protocol;
            Type protocol_type = Type.getType(method_info.protocol);

            Type instance_type = Type.getType(method_info.instanceType());
            String method_owner = Type.getInternalName(method_info.instance.getClass());

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
            method_writer.visitFieldInsn(Opcodes.GETFIELD, target.internal, method_info.field_name, instance_type.getDescriptor());
            if (!protocol_type.equals(instance_type)) {
                Map.Entry<Class<?>, Method> accessable_owenr = findMehodOwner(method_info.instance.getClass(), protocol);

                // fix type
                protocol = accessable_owenr.getValue();
                protocol_type = Type.getType(protocol);
                method_owner = Type.getInternalName(accessable_owenr.getKey());

                LOGGER.info("protoco type:" + protocol_type + " instance type:" + instance_type + "accessable:" + accessable_owenr);
                method_writer.visitTypeInsn(Opcodes.CHECKCAST, Type.getInternalName(accessable_owenr.getKey()));
            }
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
                                Type.getType(AnycastOld.class).getInternalName(),
                                "bootstrapDynamic",
                                MethodType.methodType(
                                        CallSite.class, //
                                        MethodHandles.Lookup.class, //
                                        String.class, //
                                        MethodType.class, //
                                        String.class
                                ).toMethodDescriptorString(), //
                                AnycastOld.class.isInterface()
                        ),//
                        "hello"
                );
            } else {
                method_writer.visitMethodInsn(
                        Opcodes.INVOKEVIRTUAL,
                        method_owner,
                        protocol.getName(),
                        protocol_type.getDescriptor(),
                        false
                );
            }

            // end constructor
            method_writer.visitMaxs(0, 0);
            method_writer.visitInsn(protocol_type.getReturnType().getOpcode(Opcodes.IRETURN));
            method_writer.visitEnd();
        });

        return;
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

    public static CallSite bootstrapDynamic(MethodHandles.Lookup caller, String name, MethodType type) {
        // ignore caller and name, but match the type:
        LOGGER.info("name:" + name + " type:" + type);
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
        Object effective_final = object;
        //make method lookup
        Arrays.stream(object.getClass().getMethods()).parallel()
                .forEach((method) -> {
                    method_lookup.compute(method.getName(), (name, collection) -> {
                        if (collection == null) {
                            collection = Queues.newConcurrentLinkedQueue();
                        }

                        collection.offer(new MethodInfoBundle(method, effective_final));
                        return collection;
                    });
                });
    }

    protected Map.Entry<Class<?>, Method> findMehodOwner(Class<?> clazz, Method method) {
        if (clazz == null) {
            return null;
        }
        LOGGER.info("testing class:" + clazz);
        if (clazz.getEnclosingClass() == null) {
            // find in this
            Optional<Method> match = Arrays.stream(clazz.getDeclaredMethods()).parallel()
                    .filter((from) -> from.getName().equals(method.getName()))
                    .filter((from) -> {
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
                    .filter((from) -> {
                        // return check
                        return from.getReturnType().isAssignableFrom(method.getReturnType());
                    })
                    .filter((from) -> {
                        // exception check
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
                    .findAny();
            if (match.isPresent()) {
                LOGGER.info("match  no enclosing method:" + method + " clazz:" + clazz);
                return Entrys.newImmutableEntry(clazz, match.get());
            }
        }

        // try parent
        Map.Entry<Class<?>, Method> found = findMehodOwner(clazz.getSuperclass(), method);
        if (found != null) {
            LOGGER.info("match method parent:" + method + " clazz:" + found);
            return found;
        }

        LOGGER.info("find in interface");
        return Arrays.stream(clazz.getInterfaces()).parallel()
                .map((interface_class) -> findMehodOwner(interface_class, method))
                .filter(Predicates.notNull())
                .findAny()
                .orElse(null);
    }

    public void printClass(byte[] clazz) {
        try {
            ClassReader reader = new ClassReader(clazz);
            reader.accept(new TraceClassVisitor(new PrintWriter(System.out)), 0);
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }

    }
}

package com.sf.misc.yarn.rpc;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.sf.misc.async.Entrys;
import com.sf.misc.async.ListenablePromise;
import com.sf.misc.async.Promises;
import io.airlift.bytecode.DynamicClassLoader;
import io.airlift.log.Logger;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import scala.Int;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AnyReference {

    public static final Logger LOGGER = Logger.get(AnyReference.class);

    protected static final DynamicClassLoader CODEGEN_CLASS_LOADER = new DynamicClassLoader(Thread.currentThread().getContextClassLoader());
    protected static final Object NULL_OBJECT = new Object() {
        @Override
        public String toString() {
            return AnyReference.class.getName() + ".NULL_OBJECT";
        }
    };

    protected final ListenablePromise<ClassWriter> writer;
    protected final Class<?> protocol;
    protected final Queue<ListenablePromise<FieldInfo>> fields;

    //protected final ListenablePromise<Set<Method>> interface_methods;
    //protected final Queue<ListenablePromise<ConcurrentMap<Method, Object>>> adopted;

    protected static class FieldInfo {
        protected final String name;
        protected final Class<?> clazz;

        public FieldInfo(String name, Class<?> clazz) {
            this.name = name;
            this.clazz = clazz;
        }
    }

    public AnyReference(Class<?> interface_type) {
        this.protocol = interface_type;
        this.fields = Queues.newConcurrentLinkedQueue();
        this.writer = createWriter().transformAsync((writer) -> {
            return collectInterfaces(protocol).transform((interfaces) -> {
                writer.visit(
                        Opcodes.V1_8,
                        Opcodes.ACC_PUBLIC,
                        Type.getType(protocol).getInternalName(),
                        null,
                        Type.getInternalName(Object.class),
                        interfaces.parallelStream() //
                                .map(Type::getInternalName) //
                                .collect(Collectors.toList()) //
                                .toArray(new String[0])
                );
                return writer;
            });
        });
    }

    public AnyReference adopt(Object object) {
        fields.offer(this.writer.transform((writer) -> {
                    String field_name = object.getClass().getName()
                            .replace(".", "_")
                            .replace(";", "")
                            .replace("[", "array_")
                            + "_" + object.hashCode();

                    // create field
                    writer.visitField(
                            Opcodes.ACC_PROTECTED | Opcodes.ACC_FINAL,
                            field_name,
                            Type.getDescriptor(object.getClass()),
                            null,
                            null
                    ).visitEnd();

                    return new FieldInfo(field_name, object.getClass());
                })
        );
        return this;
    }

    public <T> T make() {
        associate().transform((associated_methods) -> {
            return null;
        });

        return null;
    }

    protected ListenablePromise<List<FieldInfo>> collectFields() {
        //
        return null;
    }

    protected ListenablePromise<ClassWriter> createWriter() {
        return Promises.submit(() -> {
            return new ClassWriter(ClassWriter.COMPUTE_FRAMES);
        });
    }

    protected ListenablePromise<Set<Class<?>>> collectInterfaces(Class<?> boostrap) {
        return Promises.submit(() -> {
            Set<Class<?>> interfaces = Sets.newConcurrentHashSet();

            return Arrays.stream(boostrap.getInterfaces()).parallel() //
                    .map((parent_interface) -> {
                        interfaces.add(parent_interface);
                        return collectInterfaces(parent_interface);
                    })
                    .reduce(Promises.reduceCollectionsOperator())
                    .orElse(Promises.immediate(Collections.emptySet()))
                    .transform((collected) -> {
                        interfaces.addAll(collected);
                        return collected;
                    });
        }).transformAsync((throught) -> throught);
    }


    protected Class<?> codegen(ConcurrentMap<Method, Object> method_delegetes) {
        // for fixed orrder
        List<Map.Entry<Class<?>, Object>> arguments = collectArgumetns(method_delegetes);
        arguments.sort((left, right) -> left.getKey().getName().compareTo(right.getKey().getName()));

        // gen code
        CodeGen codegen = new CodeGen(this.protocol.getSimpleName(), ImmutableSet.of(this.protocol));

        // seal arguments,exclude null_object
        return null;
    }

    protected List<Map.Entry<Class<?>, Object>> collectArgumetns(ConcurrentMap<Method, Object> method_delegates) {
        return method_delegates.values().parallelStream()
                .distinct()
                .filter((object) -> object != NULL_OBJECT)
                .map((object) -> {
                    return Entrys.<Class<?>, Object>newImmutableEntry(object.getClass(), object);
                }).collect(Collectors.toList());
    }

    protected ListenablePromise<ConcurrentMap<Method, Object>> associate() {
        /*
        return this.adopted.parallelStream() //
                .map(methods -> methods.transform((instance) -> instance.entrySet()))
                .reduce(Promises.reduceCollectionsOperator())
                .orElse(Promises.immediate(Collections.emptySet()))
                .transform((methods) -> {
                    // remove duplicated methods
                    return methods.parallelStream().collect(Collectors.toConcurrentMap(
                            Map.Entry::getKey,
                            Map.Entry::getValue
                    ));
                })
                .transformAsync((methods) -> {
                    // associate instance
                    return interface_methods.transform((interface_methods) -> {
                        return interface_methods.parallelStream()
                                .map((interface_method) -> {
                                    return Entrys.newImmutableEntry(interface_method, methods.getOrDefault(interface_method, NULL_OBJECT));
                                }) //
                                .collect(new Collector<Map.Entry<Method, Object>, ConcurrentMap<Method, Set<Object>>, ConcurrentMap<Method, Object>>() {
                                    @Override
                                    public Supplier<ConcurrentMap<Method, Set<Object>>> supplier() {
                                        return Maps::newConcurrentMap;
                                    }

                                    @Override
                                    public BiConsumer<ConcurrentMap<Method, Set<Object>>, Map.Entry<Method, Object>> accumulator() {
                                        return (methods, entry) -> {
                                            methods.compute(entry.getKey(), (key, old) -> {
                                                if (old == null) {
                                                    old = Sets.newConcurrentHashSet();
                                                }

                                                old.add(entry.getValue());
                                                return old;
                                            });
                                        };
                                    }

                                    @Override
                                    public BinaryOperator<ConcurrentMap<Method, Set<Object>>> combiner() {
                                        return (left, right) -> {
                                            right.entrySet().parallelStream().forEach((entry) -> {
                                                left.compute(entry.getKey(), (key, old) -> {
                                                    if (old == null) {
                                                        old = entry.getValue();
                                                    } else {
                                                        old.addAll(entry.getValue());
                                                    }
                                                    return old;
                                                });
                                            });
                                            return left;
                                        };
                                    }

                                    @Override
                                    public Function<ConcurrentMap<Method, Set<Object>>, ConcurrentMap<Method, Object>> finisher() {
                                        return (multi_methods) -> {
                                            return multi_methods.entrySet().parallelStream() //
                                                    .map((entry) -> {
                                                        Method method = entry.getKey();
                                                        Set<Object> instances = entry.getValue();

                                                        // remove place holder
                                                        instances.remove(NULL_OBJECT);
                                                        switch (instances.size()) {
                                                            case 0:
                                                                if (method.isDefault()) {
                                                                    // default method
                                                                    return Entrys.newImmutableEntry(method, NULL_OBJECT);
                                                                }
                                                                throw new IllegalStateException("no binding for method:" + method);
                                                            case 1:
                                                                return Entrys.newImmutableEntry(method, instances.iterator().next());
                                                            default:
                                                                throw new IllegalStateException("multiple binding for method:" + method //
                                                                        + instances.stream().map(Object::toString) //
                                                                        .collect(Collectors.joining(",")) //
                                                                );
                                                        }
                                                    })
                                                    .collect(Collectors.toConcurrentMap(
                                                            Map.Entry::getKey,
                                                            Map.Entry::getValue
                                                    ));
                                        };
                                    }

                                    @Override
                                    public Set<Characteristics> characteristics() {
                                        return ImmutableSet.<Characteristics>builder() //
                                                .add(Characteristics.CONCURRENT) //
                                                .add(Characteristics.UNORDERED) //
                                                .build();
                                    }
                                });
                    });
                });
                */
        return null;
    }

    protected ListenablePromise<Set<Method>> findInteraceMethods(Class<?> maybe_interface_type) {
        return Promises.submit(() -> {
            Set<Method> methods = Sets.newConcurrentHashSet();
            if (maybe_interface_type.isInterface()) {
                Arrays.stream(maybe_interface_type.getDeclaredMethods()).parallel()
                        .forEach(methods::add);
            }

            return Stream.of(
                    maybe_interface_type.getDeclaredClasses(),
                    maybe_interface_type.getInterfaces()
            ).parallel()
                    .map((classes) -> Arrays.stream(classes).parallel())
                    .flatMap((stream) -> {
                        return stream.map((clazz) -> {
                            return clazz;
                        }).map(this::findInteraceMethods);
                    })
                    .reduce(Promises.reduceCollectionsOperator())
                    .orElse(Promises.immediate(Collections.emptySet()))
                    .transform((collected) -> {
                        methods.addAll(collected);
                        return methods;
                    });
        }).transformAsync((through) -> through);
    }
}

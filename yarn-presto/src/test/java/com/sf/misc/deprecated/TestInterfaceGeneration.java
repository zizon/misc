package com.sf.misc.deprecated;

import com.sf.misc.classloaders.ClassResolver;
import com.sf.misc.yarn.rpc.YarnRMProtocol;
import io.airlift.log.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.junit.Test;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Opcodes;
import org.weakref.jmx.internal.guava.collect.Maps;

import java.io.File;
import java.io.FileOutputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestInterfaceGeneration {

    private static Logger LOGGER = Logger.get(TestInterfaceGeneration.class);

    @Test
    public void test() throws Throwable {
        Class<?> target = ClientRMProxy.class;
        String class_name = "Gen";
        String raw_class_name = class_name.replace(".", "/");

        ClassReader reader = new ClassReader(ClassResolver.locate(target).get().openStream());
        ClassWriter writer = new ClassWriter(0);
        reader.accept(writer, 0);

        writer.visit(Opcodes.V1_8, //
                Opcodes.ACC_PUBLIC + Opcodes.ACC_ABSTRACT + Opcodes.ACC_INTERFACE, //
                raw_class_name, //
                null,  //
                Object.class.getName().replace(".", "/"), //
                new String[]{
                        target.getName().replace(".", "/"),
                        YarnRMProtocol.class.getName().replace(".", "/"),
                });
        //writer.visitMethod(Opcodes.ACC_PROTECTED,"checkAllowedProtocols","",)
        writer.visitEnd();

        byte[] codes = writer.toByteArray();
        URL class_root = Thread.currentThread().getContextClassLoader().getResource("");

        File output = new File(new File(class_root.toURI()), class_name.replace(".", "/") + ".class");
        output.getParentFile().mkdirs();
        new FileOutputStream(output).write(codes);
        Class<?> gen = Class.forName(class_name);

        LOGGER.info("" + class_root);
        LOGGER.info("target:" + target + " gen:" + gen + " isinstance of:" + (target.isAssignableFrom(gen)));
    }

    protected Set<Class<?>> findInterfaces(Class<?> to_infer) {
        Stream<Class<?>> indrect = Arrays.stream(to_infer.getInterfaces()) //
                .flatMap((iface) -> {
                    return findInterfaces(iface).parallelStream();
                });
        return Stream.concat(Arrays.stream(to_infer.getInterfaces()), indrect).collect(Collectors.toSet());
    }

    ;

    @Test
    public void testMethodCache() throws Throwable {
        Configuration conf = new Configuration();
        ApplicationClientProtocol client_protocol = (ApplicationClientProtocol) ClientRMProxy.createRMProxy(conf, ApplicationClientProtocol.class);
        ApplicationMasterProtocol master_protocol = (ApplicationMasterProtocol) ClientRMProxy.createRMProxy(conf, ApplicationMasterProtocol.class);

        ConcurrentMap<String, Map.Entry<Method, Object>> method_cache = Stream.of(client_protocol, master_protocol).parallel() //
                .flatMap((instance) -> {
                    return findInterfaces(instance.getClass()).parallelStream() //
                            .flatMap((iface) -> {
                                return Arrays.stream(iface.getMethods());
                            }).map((method) -> {
                                return new AbstractMap.SimpleImmutableEntry<>(method.getName(), new AbstractMap.SimpleImmutableEntry<>(method, instance));
                            });
                }) //
                .collect(
                        Maps::newConcurrentMap, //
                        (map, entry) -> {
                            map.put(entry.getKey(), entry.getValue());
                        }, //
                        Map::putAll
                );

        YarnRMProtocol object = (YarnRMProtocol) Proxy.newProxyInstance(
                Thread.currentThread().getContextClassLoader(), //
                new Class[]{ //
                        YarnRMProtocol.class, //
                        ApplicationClientProtocol.class, //
                        ApplicationMasterProtocol.class //
                }, //
                new InvocationHandler() {
                    @Override
                    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                        LOGGER.info("invoke:" + method.getName() + " instance:" + method_cache.get(method.getName()));

                        return null;
                    }
                });

        object.submitApplication(null);
    }
}

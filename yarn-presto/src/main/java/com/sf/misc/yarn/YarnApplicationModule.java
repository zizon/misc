package com.sf.misc.yarn;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.sf.misc.annotaions.ForOnYarn;
import io.airlift.configuration.ConfigBinder;
import io.airlift.log.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.weakref.jmx.internal.guava.collect.Maps;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.security.PrivilegedExceptionAction;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class YarnApplicationModule implements Module {

    public static Logger LOGGER = Logger.get(YarnApplicationModule.class);

    @Override
    public void configure(Binder binder) {
        ConfigBinder.configBinder(binder).bindConfig(YarnApplicationConfig.class);

        binder.bind(ContainerLauncher.class).in(Scopes.SINGLETON);
        binder.bind(YarnApplication.class).in(Scopes.SINGLETON);
    }

    @Provides
    @ForOnYarn
    @Singleton
    public Configuration configuration(YarnApplicationConfig config) {
        Configuration configuration = new Configuration();
        ConfigurationGenerator generator = new ConfigurationGenerator();

        // hdfs ha
        config.getNameservices().forEach((uri) -> {
            generator.generateHdfsHAConfiguration(uri).entrySet() //
                    .forEach((entry) -> {
                        configuration.set(entry.getKey(), entry.getValue());
                    });
        });

        // resource managers
        generator.generateYarnConfiguration(config.getResourceManagers()).entrySet() //
                .forEach((entry) -> {
                    configuration.set(entry.getKey(), entry.getValue());
                });

        return configuration;
    }

    @Provides
    @ForOnYarn
    @Singleton
    public UserGroupInformation ugi(YarnApplicationConfig config) {
        return UserGroupInformation.createProxyUser(config.getProxyUser(), UserGroupInformation.createRemoteUser(config.getRealUser()));
    }

    @Provides
    @ForOnYarn
    @Singleton
    public YarnRMProtocol yarnRMProtocol(@ForOnYarn Configuration conf, @ForOnYarn UserGroupInformation ugi) throws Exception {
        return ugi.doAs(new PrivilegedExceptionAction<YarnRMProtocol>() {
            @Override
            public YarnRMProtocol run() throws Exception {
                ConcurrentMap<String, Map.Entry<Method, Object>> method_cache = Stream.of( //
                        ClientRMProxy.createRMProxy(conf, ApplicationClientProtocol.class), //
                        ClientRMProxy.createRMProxy(conf, ApplicationMasterProtocol.class)).parallel() //
                        .flatMap((instance) -> {
                            return findInterfaces(instance.getClass()).parallelStream() //
                                    .flatMap((iface) -> {
                                        return Arrays.stream(iface.getMethods());
                                    }).map((method) -> {
                                        return new AbstractMap.SimpleImmutableEntry<>(method.getName(), new AbstractMap.SimpleImmutableEntry<>(method, instance));
                                    });
                        }).collect(
                                Maps::newConcurrentMap, //
                                (map, entry) -> {
                                    map.put(entry.getKey(), entry.getValue());
                                }, //
                                Map::putAll
                        );

                return (YarnRMProtocol) Proxy.newProxyInstance(
                        Thread.currentThread().getContextClassLoader(), //
                        new Class[]{ //
                                YarnRMProtocol.class, //
                                ApplicationClientProtocol.class, //
                                ApplicationMasterProtocol.class //
                        }, //
                        new InvocationHandler() {
                            @Override
                            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                                Map.Entry<Method, Object> method_instance = method_cache.get(method.getName());
                                return ugi.doAs( //
                                        (PrivilegedExceptionAction<Object>) () -> method_instance.getKey()//
                                                .invoke(method_instance.getValue(), args)
                                );
                            }
                        });
            }
        });
    }

    protected Set<Class<?>> findInterfaces(Class<?> to_infer) {
        Stream<Class<?>> indrect = Arrays.stream(to_infer.getInterfaces()) //
                .flatMap((iface) -> {
                    return findInterfaces(iface).parallelStream();
                });
        return Stream.concat(Arrays.stream(to_infer.getInterfaces()), indrect).collect(Collectors.toSet());
    }
}

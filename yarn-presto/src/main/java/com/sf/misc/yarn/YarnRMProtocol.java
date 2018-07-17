package com.sf.misc.yarn;

import com.google.common.collect.Maps;
import com.google.common.reflect.Reflection;
import com.sf.misc.async.Entrys;
import com.sf.misc.async.ListenablePromise;
import com.sf.misc.async.Promises;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.client.ClientRMProxy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.security.PrivilegedExceptionAction;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface YarnRMProtocol extends ApplicationMasterProtocol, ApplicationClientProtocol, ConfigurationAware<YarnRMProtocolConfig>, UGIAware {

    public static ListenablePromise<YarnRMProtocol> create(YarnRMProtocolConfig conf) {
        UserGroupInformation ugi = UserGroupInformation.createProxyUser( //
                conf.getProxyUser(), //
                UserGroupInformation.createRemoteUser(conf.getRealUser()) //
        );
        return Promises.submit(() -> ugi.doAs(new PrivilegedExceptionAction<YarnRMProtocol>() {
            @Override
            public YarnRMProtocol run() throws Exception {
                Configuration configuration = new Configuration();
                new ConfigurationGenerator().generateYarnConfiguration(conf.getRMs()).entrySet() //
                        .forEach((entry) -> {
                                    configuration.set(entry.getKey(), entry.getValue());
                                } //
                        );
                ConcurrentMap<String, Map.Entry<Method, Object>> method_cache = Stream.of( //
                        ClientRMProxy.createRMProxy(configuration, ApplicationClientProtocol.class), //
                        ClientRMProxy.createRMProxy(configuration, ApplicationMasterProtocol.class), //
                        new ConfigurationAware<YarnRMProtocolConfig>() {
                            @Override
                            public YarnRMProtocolConfig config() {
                                return conf;
                            }
                        },
                        new UGIAware() {
                            @Override
                            public UserGroupInformation ugi() {
                                return ugi;
                            }
                        }
                ).parallel().flatMap((instance) -> {
                    return findInterfaces(instance.getClass()).parallelStream() //
                            .flatMap((iface) -> {
                                return Arrays.stream(iface.getMethods());
                            }).map((method) -> {
                                return Entrys.newImmutableEntry(method.getName(), new AbstractMap.SimpleImmutableEntry<>(method, instance));
                            });
                }).collect(
                        Maps::newConcurrentMap, //
                        (map, entry) -> {
                            map.put(entry.getKey(), entry.getValue());
                        }, //
                        Map::putAll
                );

                return Reflection.newProxy(YarnRMProtocol.class,
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
        }));
    }

    static Set<Class<?>> findInterfaces(Class<?> to_infer) {
        Stream<Class<?>> indrect = Arrays.stream(to_infer.getInterfaces()) //
                .flatMap((iface) -> {
                    return findInterfaces(iface).parallelStream();
                });
        return Stream.concat(Arrays.stream(to_infer.getInterfaces()), indrect).collect(Collectors.toSet());
    }
}

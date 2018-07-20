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
                return new ProtocolProxy<>(YarnRMProtocol.class, new Object[]{
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
                }).make(ugi);
            }
        }));
    }
}

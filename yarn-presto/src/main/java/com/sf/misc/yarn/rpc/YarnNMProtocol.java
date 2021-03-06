package com.sf.misc.yarn.rpc;

import com.google.common.reflect.Reflection;
import com.sf.misc.async.ListenablePromise;
import com.sf.misc.async.Promises;
import com.sf.misc.bytecode.AnyCast;
import com.sf.misc.yarn.ConfigurationAware;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersResponse;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.security.PrivilegedAction;
import java.util.stream.Stream;

public interface YarnNMProtocol extends ContainerManagementProtocol, ConfigurationAware<Configuration>, UGIAware, YarnRPCAware, AutoCloseable {

    public static ListenablePromise<YarnNMProtocol> create(UserGroupInformation ugi, Configuration configuration) {
        YarnRPC rpc = ugi.doAs((PrivilegedAction<YarnRPC>) () -> YarnRPC.create(configuration));

        return Promises.submit(() -> {
            return new ProtocolProxy<>(
                    YarnNMProtocol.class,
                    new Object[]{
                            new ConfigurationAware<Configuration>() {
                                @Override
                                public Configuration config() {
                                    return configuration;
                                }
                            },
                            new UGIAware() {
                                @Override
                                public UserGroupInformation ugi() {
                                    return ugi;
                                }
                            },
                            new YarnRPCAware() {
                                @Override
                                public YarnRPC rpc() {
                                    return rpc;
                                }
                            }
                    }
            ).make(ugi);
        });
    }

    default ListenablePromise<YarnNMProtocol> connect(String host, int port) {
        return Promises.submit(() -> {
            return new ProtocolProxy<>(
                    YarnNMProtocol.class, //
                    new Object[]{
                            this.doAS(() -> {
                                return rpc().getProxy( //
                                        ContainerManagementProtocol.class, //
                                        new InetSocketAddress( //
                                                host, //
                                                port //
                                        ),
                                        config() //
                                );
                            }),
                            new ConfigurationAware<Configuration>() {
                                @Override
                                public Configuration config() {
                                    return config();
                                }
                            },
                            new UGIAware() {
                                @Override
                                public UserGroupInformation ugi() {
                                    return ugi();
                                }
                            },
                            new YarnRPCAware() {
                                @Override
                                public YarnRPC rpc() {
                                    return rpc();
                                }
                            }
                    } //
            ).make(ugi());
        });
    }

    default void close() throws Exception {
        this.rpc().stopProxy(this, this.config());
    }
}

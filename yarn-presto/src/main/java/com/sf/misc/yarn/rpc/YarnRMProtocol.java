package com.sf.misc.yarn.rpc;

import com.sf.misc.async.ListenablePromise;
import com.sf.misc.async.Promises;
import com.sf.misc.yarn.ConfigurationAware;
import com.sf.misc.yarn.launcher.ConfigurationGenerator;
import io.airlift.log.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.client.ClientRMProxy;

import java.security.PrivilegedExceptionAction;

public interface YarnRMProtocol extends ApplicationMasterProtocol, ApplicationClientProtocol, ConfigurationAware<YarnRMProtocolConfig>, UGIAware {

    public static final Logger LOGGER = Logger.get(YarnRMProtocol.class);

    public static ListenablePromise<YarnRMProtocol> create(YarnRMProtocolConfig conf) {
        return Promises.submit(() -> {
            UserGroupInformation proxy_user = UserGroupInformation.createProxyUser( //
                    conf.getProxyUser(), //
                    UserGroupInformation.createRemoteUser(conf.getRealUser()) //
            );
            UserGroupInformation.getCurrentUser().getCredentials().getAllTokens().forEach((token) -> {
                LOGGER.info("add token:" + token);
            });
            proxy_user.addCredentials(UserGroupInformation.getCurrentUser().getCredentials());
            return proxy_user;
        }).transform((ugi) -> ugi.doAs(new PrivilegedExceptionAction<YarnRMProtocol>() {
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

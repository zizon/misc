package com.sf.misc.yarn.rpc;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.sf.misc.async.ListenablePromise;
import com.sf.misc.async.Promises;
import com.sf.misc.yarn.ConfigurationAware;
import com.sf.misc.yarn.launcher.ConfigurationGenerator;
import io.airlift.log.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;

import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public interface YarnRMProtocol extends ApplicationMasterProtocol, ApplicationClientProtocol, ConfigurationAware<YarnRMProtocolConfig>, UGIAware {

    public static final Logger LOGGER = Logger.get(YarnRMProtocol.class);

    public static ListenablePromise<YarnRMProtocol> create(YarnRMProtocolConfig conf) {
        return Promises.submit(() -> {
            // create user
            UserGroupInformation proxy_user = UserGroupInformation.createProxyUser( //
                    conf.getProxyUser(), //
                    UserGroupInformation.createRemoteUser(conf.getRealUser()) //
            );

            Set<Token<?>> before = Sets.newConcurrentHashSet(proxy_user.getCredentials().getAllTokens());

            // inherit tokens
            proxy_user.addCredentials(UserGroupInformation.getLoginUser().getCredentials());
            proxy_user.addCredentials(UserGroupInformation.getCurrentUser().getCredentials());

            Set<Token<?>> after = Sets.newConcurrentHashSet(proxy_user.getCredentials().getAllTokens());
            Sets.difference(after, before).parallelStream().forEach((token) -> {
                LOGGER.info("add token:" + token);
            });

            return proxy_user;
        }).transform((ugi) -> ugi.doAs((PrivilegedExceptionAction<YarnRMProtocol>) () -> {
            // initialize configuration
            Configuration configuration = new Configuration();

            new ConfigurationGenerator().generateYarnConfiguration(conf).entrySet() //
                    .forEach((entry) -> {
                                configuration.set(entry.getKey(), entry.getValue());
                            } //
                    );

            // fix tokens
            UserGroupInformation.getCurrentUser().getTokens().parallelStream()
                    .filter((token) -> token.getKind().equals(AMRMTokenIdentifier.KIND_NAME))
                    .forEach((token) -> {
                        // fix amrm token service
                        token.setService(ClientRMProxy.getAMRMTokenService(configuration));
                        LOGGER.info("fix token:" + token);
                    });

            return new ProtocolProxy<>( //
                    YarnRMProtocol.class, //
                    new Object[]{
                            // master protocl should be first.
                            // since amrm token service name is fixed in this invokation,
                            // or do in manually in above token recovery.
                            ClientRMProxy.createRMProxy(configuration, ApplicationMasterProtocol.class), //
                            ClientRMProxy.createRMProxy(configuration, ApplicationClientProtocol.class), //
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
                    } //
            ).make(ugi);
        }));
    }
}

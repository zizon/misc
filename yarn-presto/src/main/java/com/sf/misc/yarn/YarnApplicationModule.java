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
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;


public class YarnApplicationModule implements Module {

    public static Logger LOGGER = Logger.get(YarnApplicationModule.class);

    @Override
    public void configure(Binder binder) {
        ConfigBinder.configBinder(binder).bindConfig(HadoopConfig.class);
        binder.bind(NMClientAsync.CallbackHandler.class).annotatedWith(ForOnYarn.class).to(YarnCallbackHandlers.class).in(Scopes.SINGLETON);
        binder.bind(AMRMClientAsync.CallbackHandler.class).annotatedWith(ForOnYarn.class).to(YarnCallbackHandlers.class).in(Scopes.SINGLETON);
        binder.bind(YarnCallbackHandlers.class).in(Scopes.SINGLETON);

        binder.bind(ContainerLauncher.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    public FileSystem hdfs(@ForOnYarn Configuration configuration) throws IOException {
        return FileSystem.get(configuration);
    }

    @Provides
    @ForOnYarn
    @Singleton
    public Configuration configuration(HadoopConfig config) {
        Configuration configuration = new Configuration();

        // resource managers
        String[] hosts = config.getResourceManagers().split(",");
        if (hosts.length > 1) {
            configuration.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);

            configuration.set(YarnConfiguration.RM_HA_IDS, Arrays.stream(hosts).map((host) -> {
                String rmid = "" + host.hashCode();
                configuration.set(YarnConfiguration.RM_HOSTNAME + "." + rmid, host);
                return rmid;
            }).collect(Collectors.joining(",")));
        } else {
            configuration.set(YarnConfiguration.RM_ADDRESS, config.getResourceManagers());
        }

        // hdfs
        if (config.getHdfs().getHost() == null) {
            // default fs
            String nameservice = config.getHdfs().getScheme();
            configuration.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, "hdfs://" + nameservice);
            System.out.println(configuration.get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY));

            // nameservice ha provider
            configuration.set(DFSConfigKeys.DFS_NAMESERVICES, nameservice);
            configuration.setClass(DFSConfigKeys.DFS_CLIENT_FAILOVER_PROXY_PROVIDER_KEY_PREFIX + "." + nameservice, ConfiguredFailoverProxyProvider.class, FailoverProxyProvider.class);

            // set namenodes
            configuration.set(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + "." + nameservice, Arrays.stream(config.getHdfs().getAuthority().split(",")).map((host) -> {
                String namenode_id = "" + host.hashCode();
                configuration.set(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + "." + nameservice + "." + namenode_id, host);
                return namenode_id;
            }).collect(Collectors.joining(",")));

            System.out.println(configuration.get(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX));
        } else {
            // non ha
            configuration.set(FileSystem.FS_DEFAULT_NAME_KEY, "hdfs://" + config.getHdfs().getHost());
        }

        return configuration;
    }

    @Provides
    @ForOnYarn
    @Singleton
    public YarnClient yarnClient() {
        return YarnClient.createYarnClient();
    }

    @Provides
    @ForOnYarn
    @Singleton
    public AMRMClientAsync amRMClientAsync(HadoopConfig config, @ForOnYarn AMRMClientAsync.CallbackHandler handler) {
        return AMRMClientAsync.createAMRMClientAsync(
                (int) config.getPollingInterval().toMillis(), //
                handler //
        );
    }

    @Provides
    @ForOnYarn
    @Singleton
    public NMClientAsync nmClientAsync(@ForOnYarn NMClientAsync.CallbackHandler handler) {
        return NMClientAsync.createNMClientAsync(handler);
    }

    @Provides
    @Singleton
    public YarnApplication yarnApplication(@ForOnYarn Configuration configuration,
                                           @ForOnYarn AMRMClientAsync master,
                                           @ForOnYarn NMClientAsync nodes,
                                           @ForOnYarn YarnClient client
    ) {
        return new YarnApplication(configuration)//
                .withAMRMClient(master) //
                .withNMClient(nodes) //
                .withYarnClient(client) //
                ;
    }
}

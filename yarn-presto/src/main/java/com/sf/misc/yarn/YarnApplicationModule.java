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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;

import java.io.IOException;


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
        ConfigurationGenerator generator = new ConfigurationGenerator();

        // hdfs ha
        generator.generateHdfsHAConfiguration(config.getHdfs()).entrySet() //
                .forEach((entry) -> {
                    configuration.set(entry.getKey(), entry.getValue());
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
                                           @ForOnYarn YarnClient client,
                                           YarnCallbackHandlers handlers,
                                           ContainerLauncher launcher
    ) {
        handlers.add(launcher);
        return new YarnApplication(configuration)//
                .withAMRMClient(master) //
                .withNMClient(nodes) //
                .withYarnClient(client) //
                ;
    }
}

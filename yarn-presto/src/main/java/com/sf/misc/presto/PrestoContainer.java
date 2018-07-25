package com.sf.misc.presto;

import com.facebook.presto.hadoop.HadoopNative;
import com.facebook.presto.server.PrestoServer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Module;
import com.sf.misc.airlift.AirliftConfig;
import com.sf.misc.airlift.UnionDiscoveryConfig;
import com.sf.misc.airlift.UnionDiscoveryModule;
import com.sf.misc.async.ListenablePromise;
import com.sf.misc.presto.plugins.hadoop.HadoopNativePluginInstaller;
import com.sf.misc.presto.plugins.hive.HivePluginInstaller;
import com.sf.misc.yarn.ContainerConfiguration;
import com.sf.misc.yarn.rediscovery.YarnRediscoveryModule;
import io.airlift.log.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.File;
import java.net.URI;
import java.util.Collection;
import java.util.List;

public class PrestoContainer {

    public static final Logger LOGGER = Logger.get(PrestoContainer.class);

    public void main(String args[]) {
        // initialize native
        HadoopNative.requireHadoopNative();

        // a bit tricky,since plugins use different classloader,make ugi initialized
        UserGroupInformation.setConfiguration(new Configuration());

        ContainerConfiguration configuration = ContainerConfiguration.recover(System.getenv().get(ContainerConfiguration.class.getName()));
        PluginBuilder builder = PrestoConfigGenerator.newPluginBuilder(URI.create(configuration.distill(UnionDiscoveryConfig.class).getClassloader()));

        // prepare configs
        List<ListenablePromise<File>> config_files = Lists.newArrayList();

        // presto config
        ListenablePromise<File> config_file = PrestoConfigGenerator.generatePrestoConfig(configuration)
                .transform((config) -> {
                    System.setProperty("config", config.getAbsolutePath());
                    return config;
                });
        config_files.add(config_file);

        // natvie config
        Collection<ListenablePromise<File>> native_files = new HadoopNativePluginInstaller(builder).install();
        config_files.addAll(native_files);

        // hive plugin config
        Collection<ListenablePromise<File>> hive_files = new HivePluginInstaller(builder, configuration).install();
        config_files.addAll(hive_files);

        // join all config
        config_files.parallelStream()
                .map(ListenablePromise::logException)
                .forEach(ListenablePromise::unchecked);

        String container_id = System.getenv().get(ApplicationConstants.Environment.CONTAINER_ID.key());
        // then start prestor
        new PrestoServer() {
            protected Iterable<? extends Module> getAdditionalModules() {
                return ImmutableList.<Module>builder() //
                        .add(new UnionDiscoveryModule()) //
                        .add(new YarnRediscoveryModule(ConverterUtils.toContainerId(container_id)
                                .getApplicationAttemptId()
                                .getApplicationId()
                                .toString(), //
                                configuration.distill(UnionDiscoveryConfig.class) //
                        )).build();
            }
        }.run();
        ;
    }


}

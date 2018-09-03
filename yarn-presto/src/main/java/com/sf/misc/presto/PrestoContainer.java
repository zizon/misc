package com.sf.misc.presto;

import com.facebook.presto.eventlistener.EventListenerManager;
import com.facebook.presto.eventlistener.EventListenerModule;
import com.facebook.presto.execution.resourceGroups.ResourceGroupManager;
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.hadoop.HadoopNative;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.StaticCatalogStore;
import com.facebook.presto.security.AccessControlManager;
import com.facebook.presto.security.AccessControlModule;
import com.facebook.presto.server.GracefulShutdownModule;
import com.facebook.presto.server.PluginManager;
import com.facebook.presto.server.PrestoServer;
import com.facebook.presto.server.ServerConfig;
import com.facebook.presto.server.ServerMainModule;
import com.facebook.presto.server.SessionSupplier;
import com.facebook.presto.server.security.PasswordAuthenticatorManager;
import com.facebook.presto.server.security.ServerSecurityModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.sf.misc.airlift.federation.DiscoveryUpdateModule;
import com.sf.misc.airlift.federation.FederationModule;
import com.sf.misc.airlift.liveness.LivenessModule;
import com.sf.misc.async.ListenablePromise;
import com.sf.misc.presto.modules.NodeRoleModule;
import com.sf.misc.presto.plugins.PrestoServerBuilder;
import com.sf.misc.presto.plugins.hadoop.HadoopNativePluginInstaller;
import com.sf.misc.presto.plugins.hive.HivePluginInstaller;
import com.sf.misc.yarn.ContainerConfiguration;
import com.sf.misc.yarn.launcher.LauncherEnviroment;
import com.sf.misc.yarn.rediscovery.YarnRediscoveryModule;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.configuration.ConditionalModule;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.DiscoveryModule;
import io.airlift.event.client.HttpEventModule;
import io.airlift.event.client.JsonEventModule;
import io.airlift.http.server.HttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.jmx.JmxHttpModule;
import io.airlift.jmx.JmxModule;
import io.airlift.json.JsonModule;
import io.airlift.log.LogJmxModule;
import io.airlift.log.Logger;
import io.airlift.node.NodeModule;
import io.airlift.tracetoken.TraceTokenModule;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.weakref.jmx.guice.MBeanModule;

import java.io.File;
import java.net.URI;
import java.util.Collection;
import java.util.List;

public class PrestoContainer {

    public static final Logger LOGGER = Logger.get(PrestoContainer.class);

    public static void main(String args[]) {
        System.getenv().entrySet().forEach((entry) -> {
            LOGGER.info("env key:" + entry.getKey() + " value:" + entry.getValue());
        });

        // initialize native
        HadoopNative.requireHadoopNative();

        // a bit tricky,since plugins use different classloader,make ugi initialized
        UserGroupInformation.setConfiguration(new Configuration());

        // retrive container configuraiton
        ContainerConfiguration configuration = ContainerConfiguration.decode(System.getenv().get(LauncherEnviroment.CONTAINER_CONFIGURATION));

        // plugin builder
        PluginBuilder builder = PrestoConfigGenerator.newPluginBuilder(URI.create(configuration.classloader()));

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

        // then start prestor
        new PrestoServerBuilder()
                .add(new DiscoveryUpdateModule()) //
                .add(new FederationModule()) //
                .add(new YarnRediscoveryModule(configuration.group())) //
                .add(new NodeRoleModule( //
                        ConverterUtils.toContainerId(System.getenv(ApplicationConstants.Environment.CONTAINER_ID.key())), //
                        configuration.distill(PrestoContainerConfig.class).getCoordinator() //
                                ? NodeRoleModule.ContainerRole.Coordinator //
                                : NodeRoleModule.ContainerRole.Worker) //
                ) //
                .add(ConditionalModule.installModuleIf( //
                        ServerConfig.class, //
                        ServerConfig::isCoordinator, //
                        new LivenessModule() //
                        ) //
                ) //
                .build()
                .unchecked() // block and run
                .run();
        /*
        new PrestoServer() {
            protected Iterable<? extends Module> getAdditionalModules() {
                // add reduscovery if coordinator
                return ImmutableList.<Module>builder() //
                        .add(new DiscoveryUpdateModule()) //
                        .add(new FederationModule()) //
                        .add(new YarnRediscoveryModule(configuration.group())) //
                        .add(new NodeRoleModule( //
                                ConverterUtils.toContainerId(System.getenv(ApplicationConstants.Environment.CONTAINER_ID.key())), //
                                configuration.distill(PrestoContainerConfig.class).getCoordinator() //
                                        ? NodeRoleModule.ContainerRole.Coordinator //
                                        : NodeRoleModule.ContainerRole.Worker) //
                        ) //
                        .add(ConditionalModule.installModuleIf( //
                                ServerConfig.class, //
                                ServerConfig::isCoordinator, //
                                new LivenessModule() //
                                ) //
                        ) //
                        .build();
            }
        }.run();
        */
    }
}

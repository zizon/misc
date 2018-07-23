package com.sf.misc.yarn;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import com.sf.misc.airlift.Airlift;
import com.sf.misc.airlift.UnionDiscoveryConfig;
import com.sf.misc.async.ListenablePromise;
import com.sf.misc.async.Promises;
import com.sf.misc.classloaders.HttpClassLoaderModule;
import com.sf.misc.airlift.AirliftConfig;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceInventory;
import io.airlift.log.Logger;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.util.Records;

import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

public class YarnApplicationBuilder {

    public static final Logger LOGGER = Logger.get(YarnApplicationBuilder.class);

    public final String APPLICATION_TYPE = "airlift-yarn";

    protected final ListenablePromise<Airlift> airlift;
    protected final ListenablePromise<URI> classloader;
    protected final ListenablePromise<YarnRMProtocol> protocol;
    protected final LoadingCache<URI, ListenablePromise<ContainerLauncher>> enviroments;
    protected final ListenablePromise<ContainerLauncher> default_launcher;

    public YarnApplicationBuilder(AirliftConfig configuration, YarnRMProtocolConfig protocol_config) {
        this.airlift = createAirlift(configuration);

        this.classloader = createClassloader();
        this.protocol = createProtocol(protocol_config);
        this.enviroments = CacheBuilder.newBuilder() //
                .expireAfterAccess(1, TimeUnit.HOURS) //
                .build(new CacheLoader<URI, ListenablePromise<ContainerLauncher>>() {
                    @Override
                    public ListenablePromise<ContainerLauncher> load(URI key) throws Exception {
                        return createLauncer(protocol, Promises.immediate(new LauncherEnviroment(key)));
                    }
                });

        this.default_launcher = createLauncer(protocol, createLauncherEnviroment());
    }

    public ListenablePromise<ApplicationId> submitApplication(ContainerConfiguration app_config, UnionDiscoveryConfig union_config) {
        return airlift.transform((instance) -> {
            app_config.addAirliftStyleConfig(union_config);
            app_config.addAirliftStyleConfig(instance.config());

            return app_config;
        }).transformAsync(config -> {
            return this.protocol.transform((protocol) -> {
                LOGGER.info("request application...");
                GetNewApplicationResponse response = protocol.getNewApplication(GetNewApplicationRequest.newInstance());
                ApplicationId app_id = response.getApplicationId();

                // seal rm config
                return app_id;
            }).transformAsync((app_id) -> {
                // finalize it
                ListenablePromise<ContainerLauncher> launcher = launcer(union_config.getClassloader());

                return launcher.transform((instance) -> instance.enviroment()).transformAsync((enviroment) -> {
                    LOGGER.info("create application submit context for:" + app_id);
                    // prepare context
                    ApplicationSubmissionContext context = Records.newRecord
                            (ApplicationSubmissionContext.class);
                    context.setApplicationName(config.getMaster());
                    context.setApplicationId(app_id);
                    context.setApplicationType(APPLICATION_TYPE);
                    context.setKeepContainersAcrossApplicationAttempts(true);

                    return launcher.transform((instance) -> {
                        Resource resource = masterResource();
                        context.setAMContainerResourceRequest(ResourceRequest.newInstance(Priority.UNDEFINED, ResourceRequest.ANY, instance.jvmResource(resource), 1));
                        return instance.createContext(resource, config, null, null);
                    }).transformAsync((contaienr_context) -> contaienr_context)
                            .transform((container_context) -> {
                                context.setAMContainerSpec(container_context);

                                LOGGER.info("submit application:" + context);
                                protocol.unchecked().submitApplication(SubmitApplicationRequest.newInstance(context));

                                return app_id;
                            });
                });
            }).transformAsync((app_id) -> {
                return Promises.<ListenablePromise<ApplicationId>>retry(() -> {
                    ApplicationReport report = protocol.unchecked().getApplicationReport(GetApplicationReportRequest.newInstance(app_id)).getApplicationReport();
                    LOGGER.info("application report:" + report.getApplicationId() + " state:" + report.getYarnApplicationState());
                    switch (report.getYarnApplicationState()) {
                        case NEW:
                        case NEW_SAVING:
                        case SUBMITTED:
                        case ACCEPTED:
                            return Optional.empty();
                        case RUNNING:
                            return Optional.of(Promises.immediate(app_id));
                        case FAILED:
                        case KILLED:
                        case FINISHED:
                        default:
                            return Optional.of(Promises.failure(new IllegalStateException("application master not started:" + report.getDiagnostics())));
                    }
                }).transformAsync((future) -> future);
            });
        });
    }

    public ListenablePromise<UnionDiscoveryConfig> newUnionConfig() {
        return Promises.submit(() -> {
            UnionDiscoveryConfig config = new UnionDiscoveryConfig();
            return classloader.transformAsync((loader) -> {
                config.setClassloader(loader.toURL().toExternalForm());

                return this.airlift.transform((airlift) -> {
                    config.setForeignDiscovery(airlift.config().getDiscovery());
                    return config;
                });
            });
        }).transformAsync((through) -> through);
    }

    protected ListenablePromise<ContainerLauncher> launcer(String classloader) {
        ListenablePromise<ContainerLauncher> seleccted_launcher = null;
        if (classloader == null) {
            seleccted_launcher = default_launcher;
        } else {
            seleccted_launcher = enviroments.getUnchecked(URI.create(classloader));
        }

        return seleccted_launcher;
    }

    protected ListenablePromise<ContainerLauncher> createLauncer(ListenablePromise<YarnRMProtocol> protocol, ListenablePromise<LauncherEnviroment> enviroment) {
        return Promises.submit(() -> new ContainerLauncher(protocol, enviroment, true));
    }

    protected ImmutableList<Module> modules() {
        return ImmutableList.<Module>builder() //
                .add(new HttpClassLoaderModule()) //
                .build();
    }

    protected ListenablePromise<LauncherEnviroment> createLauncherEnviroment() {
        return this.classloader.transform(LauncherEnviroment::new);
    }

    protected ListenablePromise<Airlift> createAirlift(AirliftConfig configuration) {
        return Promises.submit(() -> {
            Airlift airlift = new Airlift(configuration);
            this.modules().forEach(airlift::module);

            return airlift.start();
        }).transformAsync((airlift) -> airlift);
    }

    protected ListenablePromise<URI> createClassloader() {
        return airlift.transformAsync((airlift) -> {
            ServiceInventory inventory = airlift.getInstance(ServiceInventory.class);
            return Promises.retry(() -> {
                LOGGER.info("fetcing inventory...");
                inventory.updateServiceInventory();

                StreamSupport.stream(inventory.getServiceDescriptors().spliterator(), false).forEach(descriptor -> {
                    LOGGER.info("descriptor:" + descriptor);
                });

                for (ServiceDescriptor descriptor : inventory.getServiceDescriptors(HttpClassLoaderModule.SERVICE_TYPE)) {
                    Map<String, String> properties = descriptor.getProperties();
                    URI uri = URI.create(properties.get("http-external") + properties.get("path") + "/");
                    LOGGER.info("setup http classloader:" + uri);
                    //descriptor.getProperties()
                    return Optional.of(uri);
                }

                return Optional.empty();
            });
        });
    }

    protected ListenablePromise<YarnRMProtocol> createProtocol(YarnRMProtocolConfig config) {
        return YarnRMProtocol.create(config);
    }


    protected Resource masterResource() {
        return Resource.newInstance(128, 1);
    }
}

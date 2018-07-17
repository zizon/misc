package com.sf.misc.yarn;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Module;
import com.sf.misc.airlift.Airlift;
import com.sf.misc.async.ListenablePromise;
import com.sf.misc.async.Promises;
import com.sf.misc.classloaders.HttpClassLoaderModule;
import com.sf.misc.airlift.AirliftConfig;
import com.sf.misc.configs.ApplicationSubmitConfiguration;
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
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.util.Records;

import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.StreamSupport;

public class YarnApplicationBuilder {

    public static final Logger LOGGER = Logger.get(YarnApplicationBuilder.class);

    public final String APPLICATION_TYPE = "airlift-yarn";

    protected final ListenablePromise<Airlift> airlift;
    protected final ListenablePromise<URI> classloader;
    protected final ListenablePromise<YarnRMProtocol> protocol;
    protected final ListenablePromise<LauncherEnviroment> enviroment;

    public YarnApplicationBuilder(AirliftConfig configuration, YarnRMProtocolConfig protocol_config) {
        this.airlift = createAirlift(configuration);

        this.classloader = createClassloader();
        this.enviroment = createLauncherEnviroment();
        this.protocol = createProtocol(protocol_config);
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

    protected ListenablePromise<ApplicationId> submitApplication(ApplicationSubmitConfiguration configuration) {
        return this.protocol.transform((protocol) -> {
            LOGGER.info("request application...");
            GetNewApplicationResponse response = protocol.getNewApplication(GetNewApplicationRequest.newInstance());
            ApplicationId app_id = response.getApplicationId();

            // seal rm config
            return app_id;
        }).transformAsync((app_id) -> {
            return enviroment.transformAsync((enviroment) -> {
                LOGGER.info("create application submit context for:" + app_id);
                // prepare context
                ApplicationSubmissionContext context = Records.newRecord
                        (ApplicationSubmissionContext.class);
                context.setApplicationName(configuration.getMaster());
                context.setApplicationId(app_id);
                context.setApplicationType(APPLICATION_TYPE);
                context.setKeepContainersAcrossApplicationAttempts(true);

                Resource resource = masterResource();
                context.setAMContainerResourceRequest(ResourceRequest.newInstance(Priority.UNDEFINED, ResourceRequest.ANY, resource, 1));

                ImmutableMap<String, String> merged = ImmutableMap.<String, String>builder() //
                        .putAll(enviroment.enviroments())
                        .put(ApplicationSubmitConfiguration.class.getName(), //
                                ApplicationSubmitConfiguration.embedded(configuration) //
                        )//
                        .build();

                return enviroment.launcherCommand(
                        jvmResource(resource), //
                        Collections.emptyMap(), //
                        Class.forName(configuration.getMaster()) //
                ).transform((command) -> {
                    context.setAMContainerSpec(ContainerLaunchContext.newInstance(null, merged, command, null, null, null));

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
    }

    protected Resource masterResource() {
        return Resource.newInstance(128, 1);
    }

    protected Resource jvmResource(Resource resource) {
        return Resource.newInstance((int) (resource.getMemory() * 1.2), resource.getVirtualCores());
    }
}

package com.sf.misc.yarn;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.inject.Module;
import com.sf.misc.airlift.Airlift;
import com.sf.misc.async.ListenablePromise;
import com.sf.misc.async.Promises;
import com.sf.misc.classloaders.HttpClassLoaderModule;
import com.sf.misc.airlift.AirliftConfig;
import com.sf.misc.yarn.launcher.ContainerLauncher;
import com.sf.misc.yarn.launcher.LauncherEnviroment;
import com.sf.misc.yarn.rpc.YarnRMProtocol;
import com.sf.misc.yarn.rpc.YarnRMProtocolConfig;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceInventory;
import io.airlift.log.Logger;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
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
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

public class YarnApplicationBuilder {

    public static final Logger LOGGER = Logger.get(YarnApplicationBuilder.class);

    public final String APPLICATION_TYPE = "airlift-yarn";

    protected final ListenablePromise<Airlift> airlift;
    protected final ListenablePromise<YarnRMProtocol> master;
    protected final LoadingCache<URI, ListenablePromise<ContainerLauncher>> launchers;
    protected final ListenablePromise<ContainerLauncher> default_launcher;

    public YarnApplicationBuilder(AirliftConfig configuration, YarnRMProtocolConfig protocol_config) {
        // prepare airlift
        this.airlift = createAirlift(configuration);

        // prepare rm protocol
        this.master = createRMProtocol(protocol_config);

        // default launcher
        this.default_launcher = createLauncer(master, createDefaultLauncherEnviroment());

        // non default launcher
        this.launchers = CacheBuilder.newBuilder() //
                .expireAfterAccess(1, TimeUnit.HOURS) //
                .build(new CacheLoader<URI, ListenablePromise<ContainerLauncher>>() {
                    @Override
                    public ListenablePromise<ContainerLauncher> load(URI classloader) throws Exception {
                        return createLauncer(master, Promises.immediate(new LauncherEnviroment(Promises.immediate(classloader))));
                    }
                });
    }

    public ListenablePromise<SubmitApplicationRequest> submitApplication(ContainerConfiguration app_config) {
        LOGGER.info("submit master container with config:" + app_config.configs());

        // finialize container config
        ListenablePromise<ContainerConfiguration> container_config = updateClassloader(app_config)
                .transformAsync(this::updateApplicationMasterAirliftConfig);

        // fetch container launcher
        ListenablePromise<ContainerLauncher> launcher = container_config //
                .transform(ContainerConfiguration::classloader) //
                .transformAsync(this::selectLauncher);

        // create submission context
        ListenablePromise<ApplicationSubmissionContext> application_submission_context = container_config //
                .transformAsync(this::createApplicationSubmissionContext);

        // create application master launch context
        ListenablePromise<ContainerLaunchContext> container_launch_context = Promises.<ContainerLauncher, ContainerConfiguration, ListenablePromise<ContainerLaunchContext>>chain(launcher, container_config) //
                .call((container_launcher, config) -> {
                    return container_launcher.createContext(config);
                }).transformAsync((through) -> through);

        // creaet submit request
        ListenablePromise<SubmitApplicationRequest> request = application_submission_context.transformAsync((submission) -> {
            return launcher.transformAsync((container_launcher) -> {
                return container_launch_context.transformAsync((launcher_context) -> {
                    return container_config.transform((final_container_config) -> {
                        LOGGER.info("final container config:" + final_container_config.configs());

                        submission.setAMContainerResourceRequest( //
                                ResourceRequest.newInstance( //
                                        Priority.UNDEFINED, //
                                        ResourceRequest.ANY, //
                                        container_launcher.jvmOverhead( //
                                                Resource.newInstance( //
                                                        final_container_config.getMemory(), //
                                                        final_container_config.getCpu() //
                                                ) //
                                        ),  //
                                        1) //
                        );
                        LOGGER.info("master resource reqeust:" + submission.getAMContainerResourceRequest());

                        // finalizer applicaiton context
                        submission.setAMContainerSpec(launcher_context);

                        // submit
                        return SubmitApplicationRequest.newInstance(submission);
                    });
                });
            });
        });

        // submit and wait for submit completion
        return Promises.<YarnRMProtocol, SubmitApplicationRequest, ListenablePromise<SubmitApplicationRequest>>chain(master, request).call((master, submit_request) -> {
            // submit
            master.submitApplication(submit_request);

            // wait for ready
            GetApplicationReportRequest report_request = GetApplicationReportRequest.newInstance(submit_request.getApplicationSubmissionContext().getApplicationId());
            return Promises.<ListenablePromise<SubmitApplicationRequest>>retry(() -> {
                ApplicationReport report = master.getApplicationReport(report_request).getApplicationReport();
                LOGGER.info("application report:" + report.getApplicationId() + " state:" + report.getYarnApplicationState());
                switch (report.getYarnApplicationState()) {
                    case NEW:
                    case NEW_SAVING:
                    case SUBMITTED:
                    case ACCEPTED:
                        // not yet running,continue wait
                        return Optional.empty();
                    case RUNNING:
                        // ok,running
                        return Optional.of(Promises.immediate(submit_request));
                    case FAILED:
                    case KILLED:
                    case FINISHED:
                    default:
                        // fail when clearting application
                        return Optional.of(Promises.failure(new IllegalStateException("application master not started:" + report.getDiagnostics())));
                }
            }).transformAsync((through) -> through);
        }).transformAsync((through) -> through);
    }


    protected ListenablePromise<ContainerLauncher> selectLauncher(String classloader) {
        // if null,use default container launcher.
        // which poinst classloader to this
        if (classloader == null) {
            return default_launcher;
        }

        return launchers.getUnchecked(URI.create(classloader));
    }

    protected ListenablePromise<ContainerLauncher> createLauncer(ListenablePromise<YarnRMProtocol> protocol, ListenablePromise<LauncherEnviroment> enviroment) {
        return Promises.submit(() -> new ContainerLauncher(protocol, enviroment, true));
    }

    protected ImmutableList<Module> modules() {
        return ImmutableList.<Module>builder() //
                .add(new HttpClassLoaderModule()) //
                .build();
    }

    protected ListenablePromise<Airlift> createAirlift(AirliftConfig configuration) {
        return Promises.submit(() -> {
            Airlift airlift = new Airlift(configuration);
            this.modules().forEach(airlift::module);

            return airlift.start();
        }).transformAsync((airlift) -> airlift);
    }

    protected ListenablePromise<LauncherEnviroment> createDefaultLauncherEnviroment() {
        return airliftConfig()
                .transform(AirliftConfig::getClassloader)
                .transform(URI::new)
                .transform(Promises::immediate)
                .transform(LauncherEnviroment::new);
    }

    protected ListenablePromise<AirliftConfig> airliftConfig() {
        return airlift.transformAsync(Airlift::effectiveConfig);
    }


    protected ListenablePromise<ContainerConfiguration> updateClassloader(ContainerConfiguration container_config) {
        if (container_config.classloader() != null) {
            return Promises.immediate(container_config);
        }

        return airliftConfig().transform((this_airlift_config) -> {
            container_config.updateCloassloader(this_airlift_config.getClassloader());
            return container_config;
        });
    }

    protected ListenablePromise<ContainerConfiguration> updateApplicationMasterAirliftConfig(ContainerConfiguration container_config) {
        return airliftConfig().transform((this_airlift_config) -> {
            AirliftConfig config = new AirliftConfig();

            // keep classloader agreed
            config.setClassloader(container_config.classloader());

            // use same federation
            config.setFederationURI(this_airlift_config.getFederationURI());

            // use same env
            config.setNodeEnv(this_airlift_config.getNodeEnv());

            // ignore others

            // encode
            container_config.addAirliftStyleConfig(config);
            return container_config;
        });
    }

    protected ListenablePromise<ApplicationSubmissionContext> createApplicationSubmissionContext(ContainerConfiguration container_config) {
        return this.master.transform((master) -> {
            LOGGER.info("request application...");
            // new applicaiton id
            ApplicationId app_id = master.getNewApplication( //
                    GetNewApplicationRequest.newInstance() //
            ).getApplicationId();

            // prepare applicaiton context
            ApplicationSubmissionContext context = Records.newRecord
                    (ApplicationSubmissionContext.class);
            context.setApplicationName(container_config.getMaster());
            context.setApplicationId(app_id);
            context.setApplicationType(APPLICATION_TYPE);
            context.setKeepContainersAcrossApplicationAttempts(true);
            context.setMaxAppAttempts(3);

            return context;
        });
    }

    protected ListenablePromise<YarnRMProtocol> createRMProtocol(YarnRMProtocolConfig config) {
        return YarnRMProtocol.create(config);
    }
}

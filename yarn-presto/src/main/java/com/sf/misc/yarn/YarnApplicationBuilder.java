package com.sf.misc.yarn;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
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
                    public ListenablePromise<ContainerLauncher> load(URI key) throws Exception {
                        return createLauncer(master, Promises.immediate(new LauncherEnviroment(Promises.immediate(key))));
                    }
                });
    }

    public ListenablePromise<ApplicationSubmissionContext> submitApplication(ContainerConfiguration app_config) {
        return airlift.transform((airlift) -> {
            // offer airlift config
            app_config.addAirliftStyleConfig(airlift.config());

            return app_config;
        }).transformAsync(config -> {
            return this.master.transform((master) -> {
                LOGGER.info("request application...");
                // new applicaiton id
                ApplicationId app_id = master.getNewApplication( //
                        GetNewApplicationRequest.newInstance() //
                ).getApplicationId();

                // prepare applicaiton context
                ApplicationSubmissionContext context = Records.newRecord
                        (ApplicationSubmissionContext.class);
                context.setApplicationName(config.getMaster());
                context.setApplicationId(app_id);
                context.setApplicationType(APPLICATION_TYPE);
                context.setKeepContainersAcrossApplicationAttempts(true);

                return context;
            }).transformAsync((application_context) -> {
                // select launcher by classloader
                return selectLauncher(app_config.classloader()) //
                        .transformAsync((launcher) -> {
                            // prepare master container resoruces request
                            application_context.setAMContainerResourceRequest( //
                                    ResourceRequest.newInstance( //
                                            Priority.UNDEFINED, //
                                            ResourceRequest.ANY, //
                                            launcher.jvmOverhead( //
                                                    Resource.newInstance( //
                                                            app_config.getCpu(),  //
                                                            app_config.getMemory() //
                                                    ) //
                                            ),  //
                                            1) //
                            );

                            // setup master container launcher context
                            return launcher.createContext(app_config) //
                                    .transformAsync((container_context) -> {
                                        // finalizer applicaiton context
                                        application_context.setAMContainerSpec(container_context);

                                        // do submit
                                        return master.transform((master) -> {
                                            master.submitApplication(SubmitApplicationRequest.newInstance(application_context));
                                            return application_context;
                                        });
                                    });
                        });
            }).transformAsync((application_context) -> {
                // wait application state transiston
                GetApplicationReportRequest request = GetApplicationReportRequest.newInstance(application_context.getApplicationId());
                return master.transformAsync((master) -> {
                    return Promises.<ListenablePromise<ApplicationSubmissionContext>>retry(() -> {
                        ApplicationReport report = master.getApplicationReport(request).getApplicationReport();
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
                                return Optional.of(Promises.immediate(application_context));
                            case FAILED:
                            case KILLED:
                            case FINISHED:
                            default:
                                // fail when clearting application
                                return Optional.of(Promises.failure(new IllegalStateException("application master not started:" + report.getDiagnostics())));
                        }
                    }).transformAsync((through) -> through);
                });
            });
        });
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
        return airlift.transform((airlift) -> {
            return new LauncherEnviroment(Promises.immediate(URI.create(airlift.config().getClassloader())));
        });
    }

    protected ListenablePromise<YarnRMProtocol> createRMProtocol(YarnRMProtocolConfig config) {
        return YarnRMProtocol.create(config);
    }
}

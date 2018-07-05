package com.sf.misc.yarn;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import com.sf.misc.annotaions.ForOnYarn;
import com.sf.misc.async.ExecutorServices;
import com.sf.misc.async.Functional;
import com.sf.misc.async.FutureExecutor;
import io.airlift.discovery.client.DiscoveryClientConfig;
import io.airlift.log.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.Records;

import java.net.InetAddress;
import java.net.URI;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.Callable;

public class YarnApplication {

    public static final Logger LOGGER = Logger.get(YarnApplication.class);

    public static final String APPLICATION_TYPE = "unmanaged-next";

    protected final Configuration configuration;
    protected final YarnRMProtocol master;
    protected final UserGroupInformation ugi;
    protected final URI tracking;
    protected final String application_name;
    protected final SettableFuture<ApplicationId> application;


    @Inject
    public YarnApplication(@ForOnYarn Configuration configuration, //
                           @ForOnYarn UserGroupInformation ugi, //
                           @ForOnYarn YarnRMProtocol master,
                           DiscoveryClientConfig discovery,
                           YarnApplicationConfig hadoop_config
    ) {
        this.configuration = configuration;
        this.ugi = ugi;
        this.master = master;
        this.tracking = discovery.getDiscoveryServiceURI();
        this.application_name = hadoop_config.getApplicaitonName();

        this.application = createNew();
    }

    public ListenableFuture<ApplicationId> getApplication() {
        return this.application;
    }

    protected ListeningExecutorService executor() {
        return ExecutorServices.executor();
    }

    protected SettableFuture<ApplicationId> createNew() {
        SettableFuture<ApplicationId> result = SettableFuture.create();

        // submit application
        ListenableFuture<ApplicationId> application = executor().submit(() -> {
            LOGGER.info("request application...");
            GetNewApplicationResponse response = master.getNewApplication(GetNewApplicationRequest.newInstance());
            ApplicationId app_id = response.getApplicationId();

            LOGGER.info("create application submit context for:" + app_id);

            // prepare context
            ApplicationSubmissionContext context = Records.newRecord
                    (ApplicationSubmissionContext.class);
            context.setApplicationName(application_name);
            context.setApplicationId(app_id);
            context.setApplicationType(APPLICATION_TYPE);
            context.setKeepContainersAcrossApplicationAttempts(true);

            // set appmaster launch spec
            context.setAMContainerSpec(ContainerLaunchContext.newInstance(null, null, null, null, null, null));

            // unmanaged app master
            context.setUnmanagedAM(true);

            // submit
            LOGGER.info("submit application:" + context);
            master.submitApplication(SubmitApplicationRequest.newInstance(context));
            return app_id;
        });

        // find master token
        ListenableFuture<RegisterApplicationMasterResponse> master_register = //
                Futures.whenAllComplete(application).callAsync(() -> {
                            YarnRMProtocol protocol = master;
                            ApplicationId app_id = application.get();
                            GetApplicationReportRequest request = GetApplicationReportRequest.newInstance(app_id);
                            for (; ; ) {
                                GetApplicationReportResponse response = protocol.getApplicationReport(request);
                                switch (response.getApplicationReport().getYarnApplicationState()) {
                                    case ACCEPTED:
                                        LOGGER.info("application accepted,set master token");
                                        org.apache.hadoop.yarn.api.records.Token token = response.getApplicationReport().getAMRMToken();
                                        if (token == null) {
                                            LOGGER.info("no token found");
                                            break;
                                        }

                                        Token<AMRMTokenIdentifier> master_toekn = new org.apache.hadoop.security.token.Token<AMRMTokenIdentifier>(
                                                token.getIdentifier().array(), //
                                                token.getPassword().array(), //
                                                new Text(token.getKind()), //
                                                new Text(token.getService()) //
                                        );

                                        // add totken
                                        ugi.addToken(master_toekn);

                                        LOGGER.info("register application master with tracking url:" + this.tracking);
                                        return Futures.immediateFuture(protocol.registerApplicationMaster( //
                                                RegisterApplicationMasterRequest.newInstance( //
                                                        InetAddress.getLocalHost().getHostName(), //
                                                        this.tracking.getPort(), //
                                                        this.tracking.toURL().toExternalForm() //
                                                ) //
                                        ));
                                    case FAILED:
                                    case FINISHED:
                                        return Futures.immediateFailedFuture(new RuntimeException(response.getApplicationReport().getDiagnostics()));
                                    default:
                                        Thread.yield();
                                        break;

                                }
                            }
                        }, //
                        FutureExecutor.executor() //
                );

        FutureExecutor.addCallback(master_register, (response, throwable) -> {
            if (throwable != null) {
                result.setException(throwable);
                return;
            }

            result.setFuture(application);
        });
        return result;
    }
}

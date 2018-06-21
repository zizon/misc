package com.sf.misc.yarn;

import com.google.common.base.Function;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.sf.misc.async.ExecutorServices;
import com.sf.misc.async.Graph;
import io.airlift.log.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.PrivilegedAction;
import java.util.Optional;

public class YarnApplication {

    public static final Logger LOGGER = Logger.get(YarnApplication.class);

    /*
     *                          yarn
     *                           |
     *                      application
     *                      |        \
     *           submit-context   master-token
     *                               |
     *                             register
     *                              |
     *                             up
     */
    private static final String VERTEX_YARN = "yarn";
    private static final String VERTEX_APPLICATION = "application";
    private static final String VERTEX_SUBMIT_CONTEXT = "submit-context";
    private static final String VERTEX_MASTER_TOKEN = "master-token";
    private static final String VERTEX_MASTER = "master";
    private static final String VERTEX_NODES = "nodes";
    private static final String VERTEX_REGISTER = "register";
    private static final String VERTEX_UP = "up";

    protected Configuration configuration;
    protected YarnClient yarn;

    protected YarnClientApplication application;
    protected String application_name;
    protected Graph<ExecutorServices.Lambda> dag;
    protected Graph<ExecutorServices.Lambda> defers;
    protected AMRMClientAsync master;
    protected NMClientAsync nodes;
    protected URI tracking;
    protected ExecutorServices.Lambda whenup;
    protected String user;

    public YarnApplication(Configuration configuration) {
        this.configuration = configuration;
        this.dag = new Graph<>();
        this.defers = new Graph<>();
    }

    public YarnApplication withYarnClient(YarnClient client) {
        this.yarn = client;
        this.registerService(VERTEX_YARN, this.yarn);
        return this;
    }

    public YarnApplication withAMRMClient(AMRMClientAsync master) {
        this.master = master;
        this.registerService(VERTEX_MASTER, this.master);
        return this;
    }

    public YarnApplication withNMClient(NMClientAsync nodes) {
        this.nodes = nodes;
        this.registerService(VERTEX_NODES, this.nodes);
        return this;
    }

    public YarnApplication trackWith(URI tracking) {
        this.tracking = tracking;
        return this;
    }

    public YarnApplication withName(String application_name) {
        this.application_name = application_name;
        return this;
    }

    public YarnClientApplication getApplication() {
        return application;
    }

    public YarnApplication whenUp(ExecutorServices.Lambda lambda) {
        this.whenup = lambda;
        return this;
    }

    public YarnApplication runas(String user) {
        this.user = user;
        return this;
    }

    public ListenableFuture<YarnApplication> build() {
        return UserGroupInformation.createProxyUser(this.user, UserGroupInformation.createRemoteUser("hive")).doAs((PrivilegedAction<ListenableFuture<YarnApplication>>) () -> {
            return this.doBuild();
        });
    }

    protected ListenableFuture<YarnApplication> doBuild() {
        // VERTEX_APPLICATION
        this.dag.newVertex(VERTEX_APPLICATION,
                () -> {
                    LOGGER.info("create a application");
                    this.application = yarn.createApplication();
                    LOGGER.info("done create application");
                }) //
                // VERTEX_APPLICATION dependency
                .graph().vertex(VERTEX_YARN).link(VERTEX_APPLICATION) //
                // VERTEX_SUBMIT_CONTEXT
                .graph().newVertex(VERTEX_SUBMIT_CONTEXT, //
                () -> {
                    LOGGER.info("create application submit context");
                    ApplicationId app_id = application.getNewApplicationResponse().getApplicationId();

                    // prepare context
                    ApplicationSubmissionContext context = application.getApplicationSubmissionContext();
                    context.setApplicationName(application_name);
                    context.setApplicationId(application.getNewApplicationResponse().getApplicationId());
                    context.setApplicationType("unmanaged");
                    context.setKeepContainersAcrossApplicationAttempts(true);

                    // set appmaster launch spec
                    context.setAMContainerSpec(ContainerLaunchContext.newInstance(null, null, null, null, null, null));

                    // unmanaged app master
                    context.setUnmanagedAM(true);

                    // submit
                    yarn.submitApplication(context);
                    LOGGER.info("submit application");
                })
                //VERTEX_SUBMIT_CONTEXT dependency
                .graph().vertex(VERTEX_APPLICATION).link(VERTEX_SUBMIT_CONTEXT) //
                .graph().vertex(VERTEX_YARN).link(VERTEX_SUBMIT_CONTEXT) //
                // VERTEX_MASTER_TOKEN
                .graph().newVertex(VERTEX_MASTER_TOKEN,  //
                () -> {
                    LOGGER.info("set token");
                    UserGroupInformation.getCurrentUser().addToken(//
                            yarn.getAMRMToken(application.getNewApplicationResponse().getApplicationId()) //
                    );
                })
                // VERTEX_MASTER_TOKEN dependency
                .graph().vertex(VERTEX_APPLICATION).link(VERTEX_MASTER_TOKEN) //
                .graph().vertex(VERTEX_YARN).link(VERTEX_MASTER_TOKEN) //
                .graph().vertex(VERTEX_SUBMIT_CONTEXT).link(VERTEX_MASTER_TOKEN) //
                // VERTEX_REGISTER
                .graph().newVertex(VERTEX_REGISTER, //
                () -> {
                    LOGGER.info("register master");
                    master.registerApplicationMaster(InetAddress.getLocalHost().getHostName(), this.tracking.getPort(), this.tracking.toURL().toExternalForm());
                })
                // VERTEX_REGISTER dependency
                .graph().vertex(VERTEX_MASTER).link(VERTEX_REGISTER) //
                .graph().vertex(VERTEX_MASTER_TOKEN).link(VERTEX_REGISTER) //
                // VERTEX_UP
                .graph().newVertex(VERTEX_UP, //
                () -> {
                    Optional.ofNullable(whenup).orElse(ExecutorServices.NOOP).run();
                })
                // VERTEX_UP dependency
                .graph().vertex(VERTEX_MASTER).link(VERTEX_UP) //
                .graph().vertex(VERTEX_MASTER_TOKEN).link(VERTEX_UP) //
                .graph().vertex(VERTEX_REGISTER).link(VERTEX_UP)
        ;

        return Futures.transformAsync(ExecutorServices.submit(this.dag), (throwable) -> {
            if (throwable == null) {
                return Futures.immediateFuture(this);
            }

            return Futures.immediateFailedFuture(throwable);
        }, ExecutorServices.executor());
    }

    public ListenableFuture<YarnApplication> stop() {
        return Futures.transformAsync(ExecutorServices.submit(this.defers), (throwable) -> {
            if (throwable == null) {
                return Futures.immediateFuture(this);
            }

            return Futures.immediateFailedFuture(throwable);
        }, ExecutorServices.executor());
    }

    public YarnClient getYarn() {
        return yarn;
    }

    public AMRMClientAsync getMaster() {
        return master;
    }

    public NMClientAsync getNodes() {
        return nodes;
    }

    protected void maybeStart(AbstractService service) {
        LOGGER.info("start service:" + service);
        if (service.isInState(Service.STATE.NOTINITED)) {
            service.init(configuration);
        }

        if (service.isInState(Service.STATE.INITED)) {
            service.start();
        }
    }

    protected void registerService(String name, AbstractService service) {
        this.dag.newVertex(name, () -> maybeStart(service));
        this.defers.newVertex(name, () -> service.stop());
    }
}

package com.sf.misc.yarn;

import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.sf.misc.async.ExecutorServices;
import com.sf.misc.async.Graph;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;

import java.net.InetAddress;
import java.net.InetSocketAddress;

public class ApplicationBuilder {

    public static final Log LOGGER = LogFactory.getLog(ApplicationBuilder.class);

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
    protected InetSocketAddress rpc;
    protected String tracking;
    protected String name;
    protected ExecutorServices.Lambda whenup;

    public ApplicationBuilder(Configuration configuration) {
        this.configuration = configuration;
        this.dag = new Graph<>();
        this.defers = new Graph<>();
    }

    public ApplicationBuilder withYarnClient(YarnClient client) {
        this.yarn = client;
        this.registerService(VERTEX_YARN, this.yarn);
        return this;
    }

    public ApplicationBuilder withAMRMClient(AMRMClientAsync master) {
        this.master = master;
        this.registerService(VERTEX_MASTER, this.master);
        return this;
    }

    public ApplicationBuilder withNMClient(NMClientAsync nodes) {
        this.nodes = nodes;
        this.registerService(VERTEX_NODES, this.nodes);
        return this;
    }

    public ApplicationBuilder rpc(InetSocketAddress rpc) {
        this.rpc = rpc;
        return this;
    }

    public ApplicationBuilder trackWith(String tracking) {
        this.tracking = tracking;
        return this;
    }

    public ApplicationBuilder withName(String application_name) {
        this.name = application_name;
        return this;
    }

    public YarnClientApplication getApplication() {
        return application;
    }

    public ApplicationBuilder whenUp(ExecutorServices.Lambda lambda) {
        this.whenup = lambda;
        return this;
    }

    public ListenableFuture<ApplicationBuilder> build() {
        // VERTEX_APPLICATION
        this.dag.newVertex(VERTEX_APPLICATION,
                () -> {
                    LOGGER.info("create a application");
                    this.application = yarn.createApplication();
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
                    master.registerApplicationMaster(InetAddress.getLocalHost().getHostName(), this.rpc.getPort(), this.tracking);
                })
                // VERTEX_REGISTER dependency
                .graph().vertex(VERTEX_MASTER).link(VERTEX_REGISTER) //
                .graph().vertex(VERTEX_MASTER_TOKEN).link(VERTEX_REGISTER) //
                // VERTEX_UP
                .graph().newVertex(VERTEX_UP, whenup)
                // VERTEX_UP dependency
                .graph().vertex(VERTEX_MASTER).link(VERTEX_UP) //
                .graph().vertex(VERTEX_MASTER_TOKEN).link(VERTEX_UP) //
                .graph().vertex(VERTEX_REGISTER).link(VERTEX_UP)
        ;

        LOGGER.info("dag:" + dag.vertexs().count());
        LOGGER.info("dependency:" + dag.flip().vertexs().count());
        return Futures.transform(ExecutorServices.submit(this.dag), (Function<Boolean, ApplicationBuilder>) (ignore) -> this);
    }

    public ListenableFuture<ApplicationBuilder> stop() {
        return Futures.transform(ExecutorServices.submit(this.defers), (Function<Boolean, ApplicationBuilder>) (ignore) -> this);
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

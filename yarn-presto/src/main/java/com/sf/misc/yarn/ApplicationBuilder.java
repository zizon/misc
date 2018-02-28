package com.sf.misc.yarn;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.sf.misc.async.ExecutorServices;
import com.sf.misc.async.Graph;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;

import java.util.concurrent.ForkJoinPool;

public class ApplicationBuilder {

    public static final Log LOGGER = LogFactory.getLog(ApplicationBuilder.class);

    public static ListeningExecutorService POOL = MoreExecutors.listeningDecorator(ForkJoinPool.commonPool());

    protected Configuration configuration;
    protected YarnClient yarn;
    protected YarnClientApplication application;
    protected String application_name;
    protected Graph<ExecutorServices.Lambda> dag;


    public ApplicationBuilder(Configuration configuration) {
        this.configuration = configuration;
        this.dag = new Graph<>();
    }

    public ApplicationBuilder withYarnClient(YarnClient client) {
        this.yarn = client;
        this.dag.newVertex("yarn", () -> {
            if (client.isInState(Service.STATE.NOTINITED)) {
                this.yarn.init(configuration);
            }

            if (client.isInState(Service.STATE.INITED)) {
                this.yarn.start();
            }
        });
        return this;
    }

    public ApplicationBuilder withName(String application_name) {
        return this;
    }

    public YarnClientApplication build() {
        // prepare hook
        //build_hook.parallelStream().forEach(ExceptionFree::run);

        POOL.submit(() -> {

        });
        //TODO
        return null;
    }
}

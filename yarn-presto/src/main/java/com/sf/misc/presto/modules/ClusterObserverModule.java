package com.sf.misc.presto.modules;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.sf.misc.presto.AirliftPresto;
import com.sf.misc.presto.PrestoClusterConfig;
import io.airlift.discovery.client.DiscoveryBinder;
import io.airlift.discovery.server.DiscoveryConfig;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;

import javax.annotation.PostConstruct;

public class ClusterObserverModule implements Module {

    protected final AirliftPresto presto;
    protected final ContainerId container_id;

    public ClusterObserverModule(AirliftPresto presto, ContainerId container_id) {
        this.presto = presto;
        this.container_id = container_id;
    }

    @Override
    public void configure(Binder binder) {
        binder.bind(ClusterObserver.class).in(Scopes.SINGLETON);
        binder.bind(AirliftPresto.class).toInstance(this.presto);
        binder.bind(ContainerId.class).toInstance(container_id);
    }
}

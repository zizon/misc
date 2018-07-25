package com.sf.misc.yarn.rediscovery;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.sf.misc.airlift.UnionDiscoveryConfig;
import io.airlift.discovery.client.DiscoveryBinder;

public class YarnRediscoveryModule implements Module {

    protected String application;
    protected UnionDiscoveryConfig union_discovery_config;

    public YarnRediscoveryModule(String application, UnionDiscoveryConfig union_discovery_config) {
        this.application = application;
        this.union_discovery_config = union_discovery_config;
    }

    @Override
    public void configure(Binder binder) {
        DiscoveryBinder.discoveryBinder(binder).bindHttpAnnouncement(YarnRediscovery.SERVICE_TYPE)
                .addProperty(YarnRediscovery.APPLICATION_PROPERTY, this.application);

        binder.bind(UnionDiscoveryConfig.class).toInstance(union_discovery_config);
        binder.bind(YarnRediscovery.class).in(Scopes.SINGLETON);
    }
}

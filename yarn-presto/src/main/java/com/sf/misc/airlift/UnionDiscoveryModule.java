package com.sf.misc.airlift;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.sf.misc.async.ListenablePromise;
import com.sf.misc.async.Promises;
import io.airlift.discovery.client.Announcement;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.DiscoveryAnnouncementClient;
import io.airlift.discovery.client.DiscoveryBinder;
import io.airlift.discovery.client.ForDiscoveryClient;
import io.airlift.discovery.client.HttpDiscoveryAnnouncementClient;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.discovery.client.ServiceSelectorConfig;
import io.airlift.discovery.client.ServiceSelectorFactory;
import io.airlift.http.client.HttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;

import java.net.URI;
import java.util.concurrent.TimeUnit;

public class UnionDiscoveryModule implements Module {

    @Override
    public void configure(Binder binder) {
        DiscoveryBinder.discoveryBinder(binder)
                .bindHttpAnnouncement(UnionDiscovery.SERVICE_TYPE);

        binder.bind(UnionDiscovery.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    public UnionServiceSelector createUnionDiscoverySelector(ServiceSelectorFactory inventory_factory, NodeInfo node) {
        ServiceSelectorConfig config = new ServiceSelectorConfig();
        config.setPool(node.getPool());

        return new UnionServiceSelector( //
                ImmutableList.of(
                        inventory_factory.createServiceSelector(UnionDiscovery.SERVICE_TYPE, config), //
                        inventory_factory.createServiceSelector(UnionDiscovery.DISCOVERY_SERVICE_TYPE, config) //
                ),//
                UnionDiscovery.SERVICE_TYPE, //
                node.getPool());
    }
}

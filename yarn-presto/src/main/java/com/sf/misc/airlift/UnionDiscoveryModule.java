package com.sf.misc.airlift;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.discovery.client.DiscoveryBinder;

public class UnionDiscoveryModule implements Module {

    @Override
    public void configure(Binder binder) {
        DiscoveryBinder.discoveryBinder(binder)
                .bindHttpAnnouncement(UnionDiscovery.SERVICE_TYPE);

        binder.bind(DiscoverySelectors.class).in(Scopes.SINGLETON);
        binder.bind(UnionDiscovery.class).in(Scopes.SINGLETON);
    }
}

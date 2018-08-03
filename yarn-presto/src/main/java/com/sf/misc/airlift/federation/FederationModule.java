package com.sf.misc.airlift.federation;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.sf.misc.airlift.federation.Federation;
import io.airlift.discovery.client.DiscoveryBinder;

public class FederationModule implements Module {

    @Override
    public void configure(Binder binder) {
        DiscoveryBinder.discoveryBinder(binder)
                .bindHttpAnnouncement(Federation.SERVICE_TYPE);

        binder.bind(Federation.class).in(Scopes.SINGLETON);
        binder.bind(FederationAnnouncer.class).in(Scopes.SINGLETON);
    }
}

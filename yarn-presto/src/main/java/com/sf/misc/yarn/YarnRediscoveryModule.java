package com.sf.misc.yarn;

import com.google.inject.Binder;
import com.google.inject.Module;
import io.airlift.discovery.client.DiscoveryBinder;
import io.airlift.discovery.server.DiscoveryConfig;

public class YarnRediscoveryModule implements Module {

    protected String application;

    protected YarnRediscoveryModule(String application) {
        this.application = application;
    }

    public static YarnRediscoveryModule createNew(String application) {
        return new YarnRediscoveryModule(application);
    }

    @Override
    public void configure(Binder binder) {
        DiscoveryBinder.discoveryBinder(binder).bindHttpAnnouncement(YarnRediscovery.SERVICE_TYPE)
                .addProperty(YarnRediscovery.APPLICATION_PROPERTY, this.application);
    }
}

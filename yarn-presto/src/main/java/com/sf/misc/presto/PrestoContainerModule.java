package com.sf.misc.presto;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scope;
import com.google.inject.Scopes;
import com.sf.misc.yarn.ContainerAssurance;
import io.airlift.discovery.client.HttpDiscoveryLookupClient;
import io.airlift.discovery.client.ServiceSelector;

public class PrestoContainerModule implements Module {

    @Override
    public void configure(Binder binder) {
        binder.bind(PrestoContainerLauncher.class).in(Scopes.SINGLETON);
        binder.bind(ContainerAssurance.class).in(Scopes.SINGLETON);
    }
}

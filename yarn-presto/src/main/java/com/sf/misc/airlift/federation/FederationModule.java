package com.sf.misc.airlift.federation;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.sf.misc.airlift.AirliftConfig;
import com.sf.misc.airlift.federation.Federation;
import io.airlift.configuration.ConfigBinder;
import io.airlift.configuration.ConfigurationModule;
import io.airlift.discovery.client.DiscoveryBinder;
import io.airlift.jaxrs.JaxrsBinder;
import io.airlift.node.NodeInfo;

import java.util.Collections;
import java.util.Map;

public class FederationModule implements Module {

    @Override
    public void configure(Binder binder) {
        binder.bind(Federation.class).in(Scopes.SINGLETON);
        binder.bind(FederationAnnouncer.class).in(Scopes.SINGLETON);
        binder.bind(ServiceSelectors.class).in(Scopes.SINGLETON);

        ConfigBinder.configBinder(binder).bindConfig(AirliftConfig.class);
    }
}

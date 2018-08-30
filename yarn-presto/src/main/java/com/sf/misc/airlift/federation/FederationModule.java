package com.sf.misc.airlift.federation;

import com.facebook.presto.server.EmbeddedDiscoveryConfig;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.sf.misc.airlift.AirliftConfig;
import com.sf.misc.airlift.liveness.Liveness;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.configuration.ConfigBinder;

public class FederationModule extends AbstractConfigurationAwareModule {

    @Override
    public void setup(Binder binder) {
        binder.bind(Federation.class).in(Scopes.SINGLETON);
        binder.bind(FederationAnnouncer.class).in(Scopes.SINGLETON);
        binder.bind(ServiceSelectors.class).in(Scopes.SINGLETON);

        ConfigBinder.configBinder(binder).bindConfig(AirliftConfig.class);
    }
}

package com.sf.misc.airlift.federation;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.sf.misc.airlift.AirliftConfig;
import io.airlift.configuration.ConfigBinder;

public class FederationModule implements Module {

    @Override
    public void configure(Binder binder) {
        binder.bind(Federation.class).in(Scopes.SINGLETON);
        binder.bind(FederationAnnouncer.class).in(Scopes.SINGLETON);
        binder.bind(ServiceSelectors.class).in(Scopes.SINGLETON);

        ConfigBinder.configBinder(binder).bindConfig(AirliftConfig.class);
    }
}

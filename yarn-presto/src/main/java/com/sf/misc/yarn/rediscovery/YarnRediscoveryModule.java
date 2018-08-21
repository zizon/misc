package com.sf.misc.yarn.rediscovery;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.sf.misc.airlift.AirliftConfig;
import io.airlift.discovery.client.DiscoveryBinder;

public class YarnRediscoveryModule implements Module {

    protected final String application;

    public YarnRediscoveryModule(String application) {
        this.application = application;
    }

    @Override
    public void configure(Binder binder) {
        binder.bind(YarnRediscovery.class).in(Scopes.SINGLETON);
    }

    @YarnRediscovery.ForYarnRediscovery
    @Provides
    public String application(){
        return application;
    }

}

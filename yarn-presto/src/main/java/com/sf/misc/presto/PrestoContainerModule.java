package com.sf.misc.presto;

import com.facebook.presto.hive.metastore.thrift.StaticMetastoreConfig;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.sf.misc.yarn.ContainerAssurance;
import io.airlift.configuration.ConfigBinder;

public class PrestoContainerModule implements Module {

    @Override
    public void configure(Binder binder) {
        binder.bind(PrestoContainerLauncher.class).in(Scopes.SINGLETON);
        binder.bind(ContainerAssurance.class).in(Scopes.SINGLETON);

        ConfigBinder.configBinder(binder).bindConfig(StaticMetastoreConfig.class);
    }
}

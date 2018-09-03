package com.sf.misc.deprecated;

import com.facebook.presto.hive.HiveClientConfig;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.configuration.ConfigBinder;
import org.junit.Test;

public class TestHiveClientConfig {

    @Test
    public void test() throws Exception {
        ImmutableMap<String, String> configuration = ImmutableMap.<String, String>builder().build();
        ImmutableSet<Module> builder = ImmutableSet.<Module>builder() //
                .add(new Module() {
                    @Override
                    public void configure(Binder binder) {
                        ConfigBinder.configBinder(binder).bindConfig(HiveClientConfig.class);
                    }
                }).build();

        new Bootstrap(builder) //
                .setRequiredConfigurationProperties(configuration) //
                .strictConfig() //
                .initialize();
    }

}

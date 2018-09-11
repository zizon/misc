package com.sf.misc.presto;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.sf.misc.airlift.federation.DiscoveryUpdateModule;
import com.sf.misc.airlift.federation.FederationModule;
import com.sf.misc.airlift.liveness.LivenessModule;
import com.sf.misc.classloaders.ClassResolver;
import com.sf.misc.presto.modules.NodeRoleModule;
import com.sf.misc.yarn.rediscovery.YarnRediscoveryModule;
import io.airlift.configuration.ConditionalModule;
import io.airlift.log.Logger;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.junit.Test;
import org.objectweb.asm.ClassWriter;

public class TestPrestoContainerBuilder {

    public static final Logger LOGGER = Logger.get(TestPrestoContainerBuilder.class);

    @Test
    public void test() {
        new PrestoServerBuilder(true)
                .add(new Module() {
                    @Override
                    public void configure(Binder binder) {
                        LOGGER.info("" + binder);
                    }
                }) //
                .build()
                .unchecked();
    }
}

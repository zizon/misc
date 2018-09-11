package com.sf.misc.yarn.launcher;

import com.google.common.collect.ImmutableMap;
import com.sf.misc.yarn.rpc.YarnRMProtocolConfig;
import io.airlift.log.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Test;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

public class TestConfigurationGenerator {

    public static final Logger LOGGER = Logger.get(TestConfigurationGenerator.class);

    @Test
    public void testGenerateYarnConfiguration() {
        String rms = "10.202.77.200,10.202.77.201";
        ImmutableMap<String, String> result = new ConfigurationGenerator().generateYarnConfiguration(rms, true, ImmutableMap.<String, Integer>builder()
                .put(YarnConfiguration.RM_SCHEDULER_ADDRESS, 10086)
                .build());

        LOGGER.info("result:" + result);

        YarnRMProtocolConfig yarn_rm_protocol_config = new YarnRMProtocolConfig();
        yarn_rm_protocol_config.setRMs("10.202.77.200,10.202.77.201");
        /*
        yarn_rm_protocol_config.setPortMap(YarnConfiguration.getServiceAddressConfKeys(new Configuration()).parallelStream().map((key) -> {
            return key + ":" + key.hashCode();
        }).collect(Collectors.joining(",")));
        */
        yarn_rm_protocol_config.setPortMap(
                YarnConfiguration.RM_SCHEDULER_ADDRESS + ":" + 10086 + ","
                        + YarnConfiguration.RM_ADDRESS + ":" + 10087
        );

        result = new ConfigurationGenerator().generateYarnConfiguration(yarn_rm_protocol_config);

        result.entrySet().parallelStream()
                .forEach((entry) -> {
                    LOGGER.info("key:" + entry.getKey() + " vlaue:" + entry.getValue());
                });
    }
}

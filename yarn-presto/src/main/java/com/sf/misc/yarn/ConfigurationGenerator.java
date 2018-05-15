package com.sf.misc.yarn;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.stream.Collectors;

public class ConfigurationGenerator {

    public ImmutableMap<String, String> generateHdfsHAConfiguration(URI config) {
        // config example "test-cluster://10.202.77.200:8020,10.202.77.201:8020"
        ImmutableMap.Builder<String, String> generated = ImmutableMap.<String, String>builder();

        // hdfs
        if (config.getHost() == null) {
            // default fs
            String nameservice = config.getScheme();
            generated.put(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, "hdfs://" + nameservice);

            // nameservice ha provider
            generated.put(DFSConfigKeys.DFS_NAMESERVICES, nameservice);
            generated.put(DFSConfigKeys.DFS_CLIENT_FAILOVER_PROXY_PROVIDER_KEY_PREFIX + "." + nameservice, ConfiguredFailoverProxyProvider.class.getName());

            // set namenodes
            generated.put(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + "." + nameservice, //
                    Arrays.stream(config.getAuthority().split(",")) //
                            .map((host) -> {
                                String namenode_id = host;
                                generated.put(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + "." + nameservice + "." + namenode_id, host);
                                return namenode_id;
                            }) //
                            .collect(Collectors.joining(",")) //
            );
        } else {
            // non ha
            generated.put(FileSystem.FS_DEFAULT_NAME_KEY, "hdfs://" + config.getHost());
        }

        // hdfs implementation
        generated.put("fs.hdfs.impl", DistributedFileSystem.class.getName());
        generated.put(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, "" + new DataSize(512, DataSize.Unit.MEGABYTE).toBytes());

        return generated.build();
    }

    public ImmutableMap<String, String> generateYarnConfiguration(String rms) {
        // config example "10.202.77.200,10.202.77.201"
        ImmutableMap.Builder<String, String> generated = ImmutableMap.<String, String>builder();

        // yarn rms
        String[] hosts = rms.split(",");
        if (hosts.length > 1) {
            generated.put(YarnConfiguration.RM_HA_ENABLED, "true");

            generated.put(YarnConfiguration.RM_HA_IDS,  //
                    Arrays.stream(hosts) //
                            .map((host) -> {
                                String rmid = host;
                                generated.put(YarnConfiguration.RM_HOSTNAME + "." + rmid, host);
                                return rmid;
                            })
                            .collect(Collectors.joining(",")) //
            );
        } else {
            generated.put(YarnConfiguration.RM_ADDRESS, rms);
        }

        return generated.build();
    }
}

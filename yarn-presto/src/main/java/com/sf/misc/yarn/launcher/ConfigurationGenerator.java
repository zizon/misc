package com.sf.misc.yarn.launcher;

import com.google.common.collect.ImmutableMap;
import com.sf.misc.async.Entrys;
import com.sf.misc.yarn.rpc.YarnRMProtocolConfig;
import io.airlift.units.DataSize;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.net.URI;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
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
        return generateYarnConfiguration(rms, true, ImmutableMap.<String, Integer>builder().build());
    }

    public ImmutableMap<String, String> generateYarnConfiguration(YarnRMProtocolConfig yarn_rm_protocol_config) {
        return generateYarnConfiguration(yarn_rm_protocol_config.getRMs(),
                yarn_rm_protocol_config.getUseHttp(),

                ImmutableMap.copyOf(
                        Arrays.stream(
                                Optional.ofNullable(yarn_rm_protocol_config.getPortMap())
                                        .orElse("")
                                        .split(",")// split to server:port form
                        ).parallel()
                                .map((key_values) -> key_values.split(":"))
                                .filter((tuple) -> tuple.length == 2)
                                .collect(Collectors.toConcurrentMap(
                                        (tuple) -> tuple[0],
                                        (tuple) -> Integer.valueOf(tuple[1])
                                ))
                ));
    }

    public ImmutableMap<String, String> generateYarnConfiguration(String rms, boolean use_http, ImmutableMap<String, Integer> service_ports) {
        // config example "10.202.77.200,10.202.77.201"

        // use https?
        Configuration configuration = new Configuration();
        configuration.set(YarnConfiguration.YARN_HTTP_POLICY_KEY, use_http ? HttpConfig.Policy.HTTP_ONLY.name() : HttpConfig.Policy.HTTP_AND_HTTPS.name());

        // build services ports
        ConcurrentMap<String, Integer> fallback_service_ports = YarnConfiguration.getServiceAddressConfKeys(configuration)
                .parallelStream()
                .map((key) -> Entrys.newImmutableEntry(key, YarnConfiguration.getRMDefaultPortNumber(key, configuration)))
                .collect(Collectors.toConcurrentMap(Map.Entry::getKey, (entry) -> {
                    return service_ports.getOrDefault(entry.getKey(), entry.getValue());
                }));

        // build it
        ImmutableMap.Builder<String, String> generated = ImmutableMap.<String, String>builder();

        // yarn rms
        String[] hosts = rms.split(",");
        if (hosts.length > 1) {
            generated.put(YarnConfiguration.RM_HA_ENABLED, "true");
            generated.put(YarnConfiguration.YARN_HTTP_POLICY_KEY, configuration.get(YarnConfiguration.YARN_HTTP_POLICY_KEY));

            generated.put(YarnConfiguration.RM_HA_IDS,  //
                    Arrays.stream(hosts) //
                            .map((host) -> {
                                String rmid = host;
                                generated.put(YarnConfiguration.RM_HOSTNAME + "." + rmid, host);

                                // set service ports
                                YarnConfiguration.getServiceAddressConfKeys(configuration)
                                        .forEach((service_key) -> {
                                            String rmid_service = service_key + "." + rmid;
                                            int port = service_ports.getOrDefault(service_key,
                                                    YarnConfiguration.getRMDefaultPortNumber(service_key, configuration)
                                            );
                                            generated.put(rmid_service, host + ":" + port);
                                        });

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

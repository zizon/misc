package com.sf.misc.presto;

import com.facebook.presto.hive.metastore.thrift.StaticMetastoreConfig;
import com.facebook.presto.server.ServerMainModule;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.sf.misc.yarn.ConfigurationGenerator;
import com.sf.misc.yarn.ContainerLauncher;
import com.sf.misc.yarn.HadoopConfig;
import io.airlift.discovery.client.ServiceInventory;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.node.NodeConfig;
import org.apache.commons.collections.keyvalue.AbstractMapEntry;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Resource;

import javax.inject.Inject;
import java.io.IOException;
import java.net.URI;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PrestoContainerLauncher {

    protected ContainerLauncher launcher;
    protected ServiceInventory inventory;
    protected HttpServerInfo server_info;
    protected String enviroment;
    protected List<URI> meta_urls;
    protected List<URI> nameservices;

    public static enum MetastoreConfig {
        MetastoreURL("hive.metastore.uri");

        private final String config_key;

        MetastoreConfig(String config_key) {
            this.config_key = config_key;
        }
    }

    @Inject
    public PrestoContainerLauncher(ContainerLauncher launcher, ServiceInventory inventory, NodeConfig node_config, HttpServerInfo server_info, StaticMetastoreConfig meta_config, HadoopConfig hadoop_config) throws IOException {
        this.launcher = launcher;
        this.inventory = inventory;
        this.enviroment = node_config.getEnvironment();
        this.server_info = server_info;
        this.meta_urls = meta_config.getMetastoreUris();
        this.nameservices = hadoop_config.getNameservices();
    }

    public ListenableFuture<Container> launchContainer(ApplicationId appid, boolean coordinator) {
        Map<String, String> properties = generatePrestoConfig();
        properties.put("coordinator", Boolean.toString(coordinator));

        return launcher.launchContainer( //
                appid, //
                PrestorContainer.class, //
                Resource.newInstance(512, 1), //
                properties, //
                null //
        );
    }

    public ContainerLauncher launcher() {
        return launcher;
    }

    protected Map<String, String> generatePrestoConfig() {
        Map<String, String> properties = Maps.newConcurrentMap();
        properties.put("presto.version", ServerMainModule.class.getPackage().getImplementationVersion());
        properties.put("discovery.uri", server_info.getHttpUri().toString());
        properties.put("service-inventory.uri", properties.get("discovery.uri") + "/v1/service");
        properties.put("node.environment", enviroment);
        properties.put("http-server.http.port", "0");

        // metastore
        properties.put( //
                PrestorContainer.getYarePrestoContainerConfigKey("hive.metastore.uri"), //
                meta_urls.stream().map(URI::toString) //
                        .collect(Collectors.joining(",")) //
        );

        // hdfs name services
        ConfigurationGenerator generator = new ConfigurationGenerator();
        this.nameservices.parallelStream() //
                .map(generator::generateHdfsHAConfiguration) //
                .flatMap((map) -> map.entrySet().stream()) //
                .sequential() //
                .forEach((entry) -> {
                    properties.put( //
                            PrestorContainer.getHdfsPrestoContainerConfigKey(entry.getKey()), //
                            entry.getValue() //
                    );
                });

        return properties;
    }
}

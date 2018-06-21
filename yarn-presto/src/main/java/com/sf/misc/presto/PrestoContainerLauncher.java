package com.sf.misc.presto;

import com.facebook.presto.hive.metastore.thrift.StaticMetastoreConfig;
import com.facebook.presto.server.ServerMainModule;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.sf.misc.ranger.RangerConfig;
import com.sf.misc.yarn.ConfigurationGenerator;
import com.sf.misc.yarn.ContainerLauncher;
import com.sf.misc.yarn.HadoopConfig;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.discovery.client.ServiceInventory;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.node.NodeConfig;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Resource;

import javax.inject.Inject;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class PrestoContainerLauncher {

    protected ContainerLauncher launcher;
    protected ServiceInventory inventory;
    protected HttpServerInfo server_info;
    protected String enviroment;
    protected List<URI> meta_urls;
    protected List<URI> nameservices;
    protected RangerConfig ranger_config;

    public static enum MetastoreConfig {
        MetastoreURL("hive.metastore.uri");

        private final String config_key;

        MetastoreConfig(String config_key) {
            this.config_key = config_key;
        }
    }

    @Inject
    public PrestoContainerLauncher(ContainerLauncher launcher, ServiceInventory inventory, NodeConfig node_config, HttpServerInfo server_info, StaticMetastoreConfig meta_config, HadoopConfig hadoop_config, RangerConfig ranger_config) throws IOException {
        this.launcher = launcher;
        this.inventory = inventory;
        this.enviroment = node_config.getEnvironment();
        this.server_info = server_info;
        this.meta_urls = meta_config.getMetastoreUris();
        this.nameservices = hadoop_config.getNameservices();
        this.ranger_config = ranger_config;
    }

    public ListenableFuture<Container> launchContainer(ApplicationId appid, boolean coordinator, Optional<Integer> memory) {
        return launcher.launchContainer( //
                appid, //
                PrestoContainer.class, //
                Resource.newInstance(memory.orElse(512), 1), //
                generatePrestoConfig(coordinator), //
                null //
        );
    }

    public ContainerLauncher launcher() {
        return launcher;
    }

    protected Map<String, String> generatePrestoConfig(boolean coordinator) {
        Map<String, String> properties = Maps.newConcurrentMap();

        new ImmutableMap.Builder<String, String>() //
                .put("coordinator", Boolean.toString(coordinator))
                .put("presto.version", ServerMainModule.class.getPackage().getImplementationVersion()) //
                .put("discovery.uri", server_info.getHttpUri().toString()) //
                .put("service-inventory.uri", server_info.getHttpUri().toString() + "/v1/service") //
                .put("node.environment", enviroment) //
                .put("http-server.http.port", "0") //
                .build() //
                .entrySet().parallelStream() //
                .forEach((entry) -> {
                    properties.put(PrestoContainer.getPrestoContainerConfig(entry.getKey()), entry.getValue());
                });

        // hive catalog metastore
        new ImmutableMap.Builder<String, String>() //
                .put("hive.metastore.uri",  //
                        meta_urls.stream() //
                                .map(URI::toString) //
                                .collect(Collectors.joining(",")) //
                )//
                .build() //
                .entrySet().parallelStream() //
                .forEach((entry) -> {
                    properties.put(PrestoContainer.getHiveCatalogConfigKey(entry.getKey()), entry.getValue());
                });


        // hdfs name services
        ConfigurationGenerator generator = new ConfigurationGenerator();
        this.nameservices.parallelStream() //
                .map(generator::generateHdfsHAConfiguration) //
                .flatMap((map) -> map.entrySet().stream()) //
                .forEach((entry) -> {
                    properties.put( //
                            PrestoContainer.getHdfsConfigKey(entry.getKey()), //
                            entry.getValue() //
                    );
                });

        // ranger plugin
        new ImmutableMap.Builder<String, String>() //
                .put("ranger.policy.name", ranger_config.getPolicyName()) //
                .put("ranger.admin.url", ranger_config.getAdminURL()) //
                .put("ranger.audit.solr.url", ranger_config.getSolrURL()) //
                .put("ranger.audit.solr.collection", ranger_config.getSolrCollection()) //
                .build() //
                .entrySet().parallelStream() //
                .forEach((entry) -> {
                    properties.put(PrestoContainer.getRangerConfigKey(entry.getKey()), entry.getValue());
                });

        return properties;
    }
}

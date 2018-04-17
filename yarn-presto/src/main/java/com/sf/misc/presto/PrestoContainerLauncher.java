package com.sf.misc.presto;

import com.facebook.presto.server.ServerMainModule;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.sf.misc.yarn.ContainerLauncher;
import io.airlift.discovery.client.ServiceInventory;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.node.NodeConfig;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Resource;

import javax.inject.Inject;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.util.Map;

public class PrestoContainerLauncher {

    protected ContainerLauncher launcher;
    protected ServiceInventory inventory;
    protected HttpServerInfo server_info;
    protected String enviroment;

    @Inject
    public PrestoContainerLauncher(ContainerLauncher launcher, ServiceInventory inventory, NodeConfig node_config, HttpServerInfo server_info) throws IOException {
        this.launcher = launcher;
        this.inventory = inventory;
        this.enviroment = node_config.getEnvironment();
        this.server_info = server_info;
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
        return properties;
    }
}

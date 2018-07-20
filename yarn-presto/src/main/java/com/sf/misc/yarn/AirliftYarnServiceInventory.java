package com.sf.misc.yarn;

import com.google.inject.Inject;
import com.sf.misc.async.Promises;
import io.airlift.discovery.client.ForDiscoveryClient;
import io.airlift.discovery.client.ServiceDescriptorsRepresentation;
import io.airlift.discovery.client.ServiceInventory;
import io.airlift.discovery.client.ServiceInventoryConfig;
import io.airlift.http.client.HttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.node.NodeInfo;

import java.util.function.Supplier;

public class AirliftYarnServiceInventory {

    protected final Promises.TransformFunction<ServiceInventoryConfig, ServiceInventory> inventory;

    @Inject
    public AirliftYarnServiceInventory(NodeInfo nodeInfo,
                                       JsonCodec<ServiceDescriptorsRepresentation> serviceDescriptorsCodec,
                                       @ForDiscoveryClient HttpClient httpClient) {
        this.inventory = (config) -> new ServiceInventory(config, nodeInfo, serviceDescriptorsCodec, httpClient);
    }

    public ServiceInventory newInventory(ServiceInventoryConfig config) {
        return inventory.apply(config);
    }
}

package com.sf.misc.airlift.inventory;

import com.google.inject.Inject;
import io.airlift.discovery.client.ServiceDescriptors;
import io.airlift.discovery.client.ServiceDescriptorsRepresentation;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.discovery.client.ServiceSelectorConfig;
import io.airlift.discovery.client.ServiceSelectorFactory;
import io.airlift.node.NodeInfo;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/v1/inventory/discovery")
public class DiscoveryInventoryResource {

    protected static String DISCOVERY_SERVICE_TYPE = "discovery";

    protected final ServiceSelector discovery;
    protected final NodeInfo node;

    @Inject
    public DiscoveryInventoryResource(ServiceSelectorFactory factory, NodeInfo node) {
        this.node = node;
        // prepare selector config
        ServiceSelectorConfig config = new ServiceSelectorConfig();
        config.setPool(node.getPool());

        // filter discovery service in service mesh
        this.discovery = factory.createServiceSelector(DISCOVERY_SERVICE_TYPE, config);
    }


    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public ServiceDescriptorsRepresentation getServices() {
        return new ServiceDescriptorsRepresentation(node.getEnvironment(), discovery.selectAllServices());
    }
}

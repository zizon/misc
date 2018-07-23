package com.sf.misc.airlift;

import com.google.inject.Inject;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.discovery.client.ServiceSelectorConfig;
import io.airlift.discovery.client.ServiceSelectorFactory;
import io.airlift.node.NodeInfo;

public class DiscoverySelectors {

    public static String SERVICE_TYPE = "union-discovery";
    public static String DISCOVERY_SERVICE_TYPE = "discovery";
    public static String HTTP_URI_PROPERTY = "http-external";

    protected final ServiceSelector local_selector;
    protected final ServiceSelector foreign_selector;

    @Inject
    public DiscoverySelectors(ServiceSelectorFactory factory, NodeInfo node) {
        ServiceSelectorConfig config = new ServiceSelectorConfig();
        config.setPool(node.getPool());

        this.foreign_selector = factory.createServiceSelector(DISCOVERY_SERVICE_TYPE, config);
        this.local_selector = factory.createServiceSelector(SERVICE_TYPE, config);
    }

    public ServiceSelector foreign() {
        return foreign_selector;
    }

    public ServiceSelector local() {
        return local_selector;
    }
}

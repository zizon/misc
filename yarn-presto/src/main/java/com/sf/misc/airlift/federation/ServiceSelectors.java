package com.sf.misc.airlift.federation;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.discovery.client.ServiceSelectorConfig;
import io.airlift.discovery.client.ServiceSelectorFactory;
import io.airlift.node.NodeInfo;

import java.util.List;

public class ServiceSelectors {

    protected final ServiceSelectorConfig config;
    protected final ServiceSelectorFactory factory;
    protected final LoadingCache<String, ServiceSelector> selectors = CacheBuilder.newBuilder().build(new CacheLoader<String, ServiceSelector>() {
        @Override
        public ServiceSelector load(String type) throws Exception {
            return factory.createServiceSelector(type, config);
        }
    });

    @Inject
    public ServiceSelectors(NodeInfo node,
                            ServiceSelectorFactory factory
    ) {
        this.config = new ServiceSelectorConfig();
        this.config.setPool(node.getPool());
        this.factory = factory;
    }

    public List<ServiceDescriptor> selectServiceForType(String type) {
        return selectors.getUnchecked(type).selectAllServices();
    }

}

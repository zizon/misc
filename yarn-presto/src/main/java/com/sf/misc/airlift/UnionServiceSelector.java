package com.sf.misc.airlift;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.sf.misc.async.Promises;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceSelector;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class UnionServiceSelector implements ServiceSelector {

    protected final Collection<ServiceSelector> selectors;
    protected final String type;
    protected final String pool;

    public UnionServiceSelector(Collection<ServiceSelector> selectors, String type, String pool) {
        this.selectors = selectors;
        this.type = type;
        this.pool = pool;
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public String getPool() {
        return pool;
    }

    @Override
    public List<ServiceDescriptor> selectAllServices() {
        return selectors.parallelStream() //
                .flatMap((selector) -> selector.selectAllServices().parallelStream()) //
                .collect(Collectors.toList());
    }

    @Override
    public ListenableFuture<List<ServiceDescriptor>> refresh() {
        return selectors.parallelStream() //
                .map((selector) -> Promises.decorate(selector.refresh())) //
                .reduce(Promises.<ServiceDescriptor, List<ServiceDescriptor>>reduceCollectionsOperator())
                .orElse(Promises.immediate(Collections.emptyList()));
    }
}

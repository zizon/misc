package com.sf.misc.airlift;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.NotNull;

public class UnionDiscoveryConfig {

    protected String root_discovery;
    protected String discovery;
    protected String classloader;

    public String getRootDiscovery() {
        return root_discovery;
    }

    @Config("airlift.union.discovery.root")
    @ConfigDescription("root discovery uri")
    public void setRootDiscovery(String root_discovery) {
        this.root_discovery = root_discovery;
    }

    public String getDiscovery() {
        return discovery;
    }

    @Config("airlift.union.discovery")
    @ConfigDescription("discovery uri")
    public void setDiscovery(String discovery) {
        this.discovery = discovery;
    }

    public String getClassloader() {
        return classloader;
    }

    @Config("airlift.union.classloader")
    @ConfigDescription("cloassloader uri")
    public void setClassloader(String classloader) {
        this.classloader = classloader;
    }


}

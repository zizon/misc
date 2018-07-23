package com.sf.misc.airlift;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.NotNull;

public class UnionDiscoveryConfig {

    protected String foreign_discovery;
    protected String classloader;

    public String getForeignDiscovery() {
        return foreign_discovery;
    }

    @Config("airlift.union.discovery.foreign")
    @ConfigDescription("discovery uri")
    public void setForeignDiscovery(String foreign_discovery) {
        this.foreign_discovery = foreign_discovery;
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

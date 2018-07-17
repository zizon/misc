package com.sf.misc.airlift;

import com.sf.misc.yarn.ConfigurationAware;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.NotNull;

public class AirliftConfig implements ConfigurationAware<AirliftConfig> {

    protected String node_env;
    protected String discovery;
    protected String inventory;
    protected int port;

    @Override
    public AirliftConfig config() {
        return this;
    }

    public String getNodeEnv() {
        return node_env;
    }

    @Config("node.environment")
    @ConfigDescription("airlift node enviroment")
    @NotNull
    public void setNodeEnv(String node_env) {
        this.node_env = node_env;
    }

    public String getDiscovery() {
        return discovery;
    }

    @Config("discovery.uri")
    @ConfigDescription("airlift discovery uri")
    public void setDiscovery(String discovery) {
        this.discovery = discovery;
    }

    public String getInventory() {
        return inventory;
    }

    @Config("service-inventory.uri")
    @ConfigDescription("airlift inventory uri")
    public void setInventory(String inventory) {
        this.inventory = inventory;
    }

    public int getPort() {
        return port;
    }

    @Config("http-server.http.port")
    @ConfigDescription("airlift http listening port")
    public void setPort(int port) {
        this.port = port;
    }

}

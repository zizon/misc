package com.sf.misc.airlift;

import com.sf.misc.yarn.ConfigurationAware;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.NotNull;

public class AirliftConfig implements ConfigurationAware<AirliftConfig> {

    protected String node_env;
    protected String discovery;
    protected int port;
    protected String federation_uri;
    protected String classloader;
    protected String loglevel;


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

    public int getPort() {
        return port;
    }

    @Config("http-server.http.port")
    @ConfigDescription("airlift http listening port")
    public void setPort(int port) {
        this.port = port;
    }

    public String getFederationURI() {
        return federation_uri;
    }

    @Config("airlift.federation.bootstrap.uri")
    @ConfigDescription("federation uri")
    public void setFederationURI(String federation_uri) {
        this.federation_uri = federation_uri;
    }

    public String getClassloader() {
        return this.classloader;
    }

    @Config("airlift.classloader.bootstrap.uri")
    @ConfigDescription("discovery uri")
    public void setClassloader(String classloader) {
        this.classloader = classloader;
    }

    public String getLoglevelFile() {
        return loglevel;
    }

    @Config("log.levels-file")
    @ConfigDescription("log level files")
    public void setLoglevelFile(String loglevel) {
        this.loglevel = loglevel;
    }
}

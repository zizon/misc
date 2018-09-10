package com.sf.misc.yarn.rpc;

import com.sf.misc.yarn.ConfigurationAware;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.annotation.Nonnull;
import javax.validation.constraints.NotNull;

public class YarnRMProtocolConfig implements ConfigurationAware<YarnRMProtocolConfig> {

    private String rms;
    private String real_user;
    private String proxy_user;
    private boolean use_http = true;
    private String port_map;

    public String getRMs() {
        return rms;
    }

    @Config("yarn.rms")
    @ConfigDescription("yarn resoruce managers")
    @NotNull
    public void setRMs(String rms) {
        this.rms = rms;
    }

    public String getRealUser() {
        return real_user;
    }

    @Config("yarn.rpc.user.real")
    @ConfigDescription("yarn process user")
    @Nonnull
    public void setRealUser(String real_user) {
        this.real_user = real_user;
    }

    public String getProxyUser() {
        return proxy_user;
    }

    @Config("yarn.rpc.user.proxy")
    @ConfigDescription("rpc do as user")
    @Nonnull
    public void setProxyUser(String proxy_user) {
        this.proxy_user = proxy_user;
    }

    @Override
    public YarnRMProtocolConfig config() {
        return this;
    }


    public boolean getUseHttp() {
        return use_http;
    }

    @Config("yarn.rpc.use.http")
    @ConfigDescription("if http only")
    public void setUseHttp(boolean use_http) {
        this.use_http = use_http;
    }

    public String getPortMap() {
        return port_map;
    }

    @Config("yarn.rpc.port.map")
    @ConfigDescription("for non default service ports")
    public void setPortMap(String port_map) {
        this.port_map = port_map;
    }

}

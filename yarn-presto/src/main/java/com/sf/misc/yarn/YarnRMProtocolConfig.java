package com.sf.misc.yarn;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.annotation.Nonnull;
import javax.validation.constraints.NotNull;

public class YarnRMProtocolConfig implements ConfigurationAware<YarnRMProtocolConfig> {

    private String rms;
    private String real_user;
    private String proxy_user;

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
}

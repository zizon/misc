package com.sf.misc.yarn;

import com.google.common.collect.Lists;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import javax.annotation.Nonnull;
import javax.servlet.annotation.MultipartConfig;
import javax.validation.constraints.NotNull;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


public class YarnApplicationConfig {
    private String resource_managers;
    private List<URI> nameservices = Collections.emptyList();
    private String proxy_user;
    private String real_user;

    private String applicaiton_name;

    public String getResourceManagers() {
        return resource_managers;
    }

    @Config("yarn.rms")
    @ConfigDescription("yarn resoruce managers")
    @NotNull
    public void setResourceManagers(String resource_managers) {
        this.resource_managers = resource_managers;
    }

    public List<URI> getNameservices() {
        return nameservices;
    }

    @Config("hdfs.nameservices")
    @ConfigDescription("nameservices that should be register")
    @Nonnull
    public void setNameservices(String nameservices) {
        this.nameservices = Arrays.stream(nameservices.split(";")) //
                .map(URI::create) //
                .collect(Collectors.toList());
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

    public String getRealUser() {
        return real_user;
    }

    @Config("yarn.rpc.user.real")
    @ConfigDescription("yarn process user")
    @Nonnull
    public void setRealUser(String real_user) {
        this.real_user = real_user;
    }

    public String getApplicaitonName() {
        return applicaiton_name;
    }

    @Config("yarn.application.name")
    @ConfigDescription("yarn applicaiton name")
    @Nonnull
    public void setApplicaitonName(String applicaiton_name) {
        this.applicaiton_name = applicaiton_name;
    }
}

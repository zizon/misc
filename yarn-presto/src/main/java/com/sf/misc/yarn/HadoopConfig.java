package com.sf.misc.yarn;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;
import io.airlift.units.Duration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import javax.servlet.annotation.MultipartConfig;
import javax.validation.constraints.NotNull;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.concurrent.TimeUnit;


public class HadoopConfig {
    private Duration polling_interval = new Duration(5, TimeUnit.SECONDS);
    private String resource_managers;
    private URI hdfs;

    public URI getHdfs() {
        return hdfs;
    }

    @Config("yarn.hdfs")
    @ConfigDescription("hdfs for yarn application resources")
    @NotNull
    public void setHdfs(URI hdfs) {
        this.hdfs = hdfs;
    }

    public String getResourceManagers() {
        return resource_managers;
    }

    @Config("yarn.rms")
    @ConfigDescription("yarn resoruce managers")
    @NotNull
    public void setResourceManagers(String resource_managers) {
        this.resource_managers = resource_managers;
    }

    public Duration getPollingInterval() {
        return polling_interval;
    }

    @Config("yarn.client.polling")
    @ConfigDescription("polling intervals for yarn async cilents(ammaster/nmclient)")
    public void setPollingInterval(Duration polling_interval) {
        this.polling_interval = polling_interval;
    }


}

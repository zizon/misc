package com.sf.misc.presto.plugins.hive;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.NotNull;

public class HiveServicesConfig {
    protected String nameservives;
    protected String metastore;

    public String getMetastore() {
        return metastore;
    }

    @Config("hive.metastore.uri")
    @ConfigDescription("hive metastore uri")
    @NotNull
    public void setMetastore(String metastore) {
        this.metastore = metastore;
    }

    public String getNameservives() {
        return nameservives;
    }

    @Config("hdfs.nameservices")
    @ConfigDescription("hdfs name services")
    @NotNull
    public void setNameservives(String nameservives) {
        this.nameservives = nameservives;
    }
}

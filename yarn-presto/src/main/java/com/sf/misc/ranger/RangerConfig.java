package com.sf.misc.ranger;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.annotation.Nonnull;

public class RangerConfig {
    private String policy_name;
    private String admin_url;
    private String solr_url;
    private String solr_collection;

    public String getPolicyName() {
        return policy_name;
    }

    @Config("ranger.policy.name")
    @ConfigDescription("ranger hive policy name")
    @Nonnull
    public void setPolicyName(String policy_name) {
        this.policy_name = policy_name;
    }

    public String getAdminURL() {
        return admin_url;
    }

    @Config("ranger.admin.url")
    @ConfigDescription("ranger admin server url")
    @Nonnull
    public void setAdminURL(String admin_url) {
        this.admin_url = admin_url;
    }

    public String getSolrURL() {
        return solr_url;
    }

    @Config("ranger.audit.solr.url")
    @ConfigDescription("ranger solr audit server url")
    @Nonnull
    public void setSolrURL(String solr_url) {
        this.solr_url = solr_url;
    }

    public String getSolrCollection() {
        return solr_collection;
    }

    @Config("ranger.audit.solr.collection")
    @ConfigDescription("ranger hive policy name")
    @Nonnull
    public void setSolrCollection(String solr_collection) {
        this.solr_collection = solr_collection;
    }


}

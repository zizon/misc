package com.sf.misc.ranger;

import org.apache.hadoop.security.GroupMappingServiceProvider;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class RangerGroupMapping implements GroupMappingServiceProvider {
    @Override
    public List<String> getGroups(String user) throws IOException {
        return Collections.emptyList();
    }

    @Override
    public void cacheGroupsRefresh() throws IOException {
    }

    @Override
    public void cacheGroupsAdd(List<String> groups) throws IOException {
    }
}

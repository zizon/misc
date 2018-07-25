package com.sf.misc.presto.plugins.ranger;

import com.sf.misc.async.ListenablePromise;
import com.sf.misc.presto.plugins.PluginInstaller;

import java.io.File;
import java.util.Collection;

public class RangerPluginInstaller implements PluginInstaller {

    @Override
    public Collection<ListenablePromise<File>> install() {
        return null;
    }
}

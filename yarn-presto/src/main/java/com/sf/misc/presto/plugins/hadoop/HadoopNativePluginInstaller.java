package com.sf.misc.presto.plugins.hadoop;

import com.sf.misc.async.ListenablePromise;
import com.sf.misc.presto.PluginBuilder;
import com.sf.misc.presto.plugins.PluginInstaller;

import java.io.File;
import java.util.Collection;
import java.util.Collections;

public class HadoopNativePluginInstaller implements PluginInstaller {

    protected final PluginBuilder builder;

    public HadoopNativePluginInstaller(PluginBuilder builder) {
        this.builder = builder;
    }

    @Override
    public Collection<ListenablePromise<File>> install() {
        return Collections.singleton(builder.install(HadoopNativePlugin.class, "0"));
    }
}

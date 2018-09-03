package com.sf.misc.presto.plugins;

import com.sf.misc.async.ListenablePromise;

import java.io.File;
import java.util.Collection;

public interface PluginInstaller {

    public Collection<ListenablePromise<File>> install();
}

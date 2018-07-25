package com.sf.misc.presto.plugins.hadoop;

import com.facebook.presto.hadoop.HadoopNative;
import com.facebook.presto.spi.Plugin;

public class HadoopNativePlugin implements Plugin {
    public HadoopNativePlugin() {
        HadoopNative.requireHadoopNative();
    }
}

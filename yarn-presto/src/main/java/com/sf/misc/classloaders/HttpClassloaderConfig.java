package com.sf.misc.classloaders;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;

import javax.annotation.Nonnull;
import java.util.concurrent.TimeUnit;

public class HttpClassloaderConfig {

    private Duration class_cache_expire = new Duration(1, TimeUnit.MINUTES);

    public Duration getClassCacheExpire() {
        return class_cache_expire;
    }

    @Config("http.classloader.cache.expire")
    @ConfigDescription("cache exipre/invlidate time for classloader resoruces")
    @Nonnull
    public void setClassCacheExpire(Duration class_cache_expire) {
        this.class_cache_expire = class_cache_expire;
    }
}

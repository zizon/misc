package com.sf.misc.yarn;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.sf.misc.async.Entrys;
import com.sf.misc.async.ListenablePromise;
import com.sf.misc.async.Promises;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigurationFactory;

import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public interface ConfigurationAware<T> {
    public T config();
}

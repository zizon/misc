package com.sf.misc.airlift;

import com.sf.misc.async.Entrys;
import com.sf.misc.async.ListenablePromise;
import com.sf.misc.async.Promises;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigurationFactory;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public class AirliftPropertyTranscript {

    public static <T> T fromProperties(Map<String, String> properties, Class<T> clazz) {
        return new ConfigurationFactory(properties).build(clazz);
    }

    public static Map<String, String> toProperties(Object config) {
        // collect setter and getter
        ConcurrentMap<String, Method> methods = Arrays.stream(config.getClass().getDeclaredMethods()).parallel()
                .filter(method -> !method.isSynthetic())
                .collect(Collectors.toConcurrentMap(Method::getName, Function.identity()));

        // collect property and filed name
        return methods.entrySet().parallelStream()
                .filter((entry) -> entry.getKey().startsWith("set"))
                .map((entry) -> {
                    // config key
                    String config_key = entry.getValue().getDeclaredAnnotation(Config.class).value();

                    // find associated read method
                    String read = entry.getKey().replaceFirst("set", "get");
                    Method method = methods.get(read);
                    if (method == null) {
                        return null;
                    }

                    // then read config value
                    ListenablePromise<String> future_value = Promises.submit(() -> method.invoke(config)) //
                            .transform((value) -> {
                                return value == null ? null : String.valueOf(value);
                            });

                    return Entrys.newImmutableEntry(config_key, future_value);
                }) //
                .filter((entry) -> entry.getValue().unchecked() != null)
                .collect(Collectors.toConcurrentMap(entry -> entry.getKey(), entry -> entry.getValue().unchecked()));
    }
}

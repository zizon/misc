package com.sf.misc.yarn;

import com.facebook.presto.server.ServerConfig;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.gson.Gson;

import java.net.URI;
import java.nio.charset.Charset;
import java.util.Base64;
import java.util.Map;
import java.util.Properties;

public class ContainerConfiguration {

    protected static final Charset CHARSET = Charset.availableCharsets().get("UTF-8");
    protected static final String MASTER_KEY = "_master_class_";
    protected static final String CPU_RESOURCE_KEY = "_CPU_";
    protected static final String MEMORY_RESOURCE_KEY = "_MEMORY_";
    protected static final String CLASSLOADER_RESOURCE_KEY = "_CLASSLOADER_";
    protected static final String VERSION_KEY = "_VERSION_";
    protected static final String LOGLEVEL_KEY = "_LOG_LEVEL_";
    protected static final String GROUP_KEY = "_GROUP_KEY_";

    protected static final int DEFAULT_CPU = 1;
    protected static final int DEFAULT_MEMORY = 1024;

    protected final Map<String, String> configuraiton;

    public static String encode(ContainerConfiguration configuration) {
        // json
        String json = new Gson().toJson(configuration.configs());

        // for shell enviroment safe
        return Base64.getEncoder().encodeToString(json.getBytes(CHARSET));
    }

    public static ContainerConfiguration decode(String base64_json) {
        byte[] json = Base64.getDecoder().decode(base64_json.getBytes(CHARSET));
        String json_string = new String(json, CHARSET);

        Map<String, String> configs = new Gson().fromJson(json_string, Map.class);
        return new ContainerConfiguration(configs);
    }

    public ContainerConfiguration(Class<?> master) {
        this(master, "default-node-group-" + System.currentTimeMillis() + "-" + master.hashCode(), DEFAULT_CPU, DEFAULT_MEMORY, null, new Properties());
    }

    public ContainerConfiguration(Class<?> master, String group, int cpu, int memory, String classloader, Properties log_levels) {
        this(Maps.newConcurrentMap());
        this.configuraiton.put(MASTER_KEY, master.getName());
        this.configuraiton.put(CPU_RESOURCE_KEY, String.valueOf(cpu));
        this.configuraiton.put(MEMORY_RESOURCE_KEY, String.valueOf(memory));
        this.configuraiton.put(LOGLEVEL_KEY, new Gson().toJson(log_levels));
        this.configuraiton.put(GROUP_KEY, group);
        updateCloassloader(classloader);
    }

    protected ContainerConfiguration(Map<String, String> configuraiton) {
        this.configuraiton = configuraiton;
    }

    public String getMaster() {
        return configuraiton.get(MASTER_KEY);
    }

    public int getCpu() {
        return Integer.valueOf(configuraiton.getOrDefault(CPU_RESOURCE_KEY, "" + DEFAULT_CPU));
    }

    public int getMemory() {
        return Integer.valueOf(configuraiton.getOrDefault(MEMORY_RESOURCE_KEY, "" + DEFAULT_MEMORY));
    }

    public String classloader() {
        return configuraiton.get(CLASSLOADER_RESOURCE_KEY);
    }

    public Properties logLevels() {
        Properties properties = new Properties();
        String raw = this.configuraiton.getOrDefault(LOGLEVEL_KEY, null);
        if (raw != null) {
            properties.putAll(new Gson().fromJson(raw, Map.class));
        }
        return properties;
    }

    public String group() {
        return this.configuraiton.get(GROUP_KEY);
    }

    public ContainerConfiguration updateCloassloader(String classloader) {
        if (classloader != null) {
            this.configuraiton.put(CLASSLOADER_RESOURCE_KEY, classloader);
        }
        return this;
    }

    public Map<String, String> configs() {
        return ImmutableMap.<String, String>builder().putAll(this.configuraiton).build();
    }

    public <T> ContainerConfiguration addAirliftStyleConfig(T object) {
        this.configuraiton.put(object.getClass().getName(), new Gson().toJson(object));
        return this;
    }

    public <T> T distill(Class<T> clazz) {
        return new Gson().fromJson(this.configuraiton.get(clazz.getName()), clazz);
    }

    @Override
    public String toString() {
        return configs().toString();
    }

}

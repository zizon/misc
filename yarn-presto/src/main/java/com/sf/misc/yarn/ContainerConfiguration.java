package com.sf.misc.yarn;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.gson.Gson;

import java.net.URI;
import java.nio.charset.Charset;
import java.util.Base64;
import java.util.Map;

public class ContainerConfiguration {

    protected static final Charset CHARSET = Charset.availableCharsets().get("UTF-8");
    protected static final String MASTER_KEY = "_master_class_";
    protected static final String CPU_RESOURCE_KEY = "_CPU_";
    protected static final String MEMORY_RESOURCE_KEY = "_MEMORY_";
    protected static final String CLASSLOADER_RESOURCE_KEY = "_CLASSLOADER_";

    protected static final int DEFAULT_CPU = 1;
    protected static final int DEFAULT_MEMORY = 1024;

    protected final Map<String, String> configuraiton;

    public ContainerConfiguration(Class<?> master) {
        this(master, DEFAULT_CPU, DEFAULT_MEMORY, null);
    }

    public ContainerConfiguration(Class<?> master, int cpu, int memory, String classloader) {
        this(Maps.newConcurrentMap());
        this.configuraiton.put(MASTER_KEY, master.getName());
        this.configuraiton.put(CPU_RESOURCE_KEY, String.valueOf(cpu));
        this.configuraiton.put(MEMORY_RESOURCE_KEY, String.valueOf(memory));
        this.configuraiton.put(CLASSLOADER_RESOURCE_KEY, classloader);
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

    public static String encode(ContainerConfiguration configuration) {
        // json
        String json = new Gson().toJson(configuration.configs());

        return Base64.getEncoder().encodeToString(json.getBytes(CHARSET));
    }

    public static ContainerConfiguration decode(String base64_json) {
        byte[] json = Base64.getDecoder().decode(base64_json.getBytes(CHARSET));
        String json_string = new String(json, CHARSET);

        Map<String, String> configs = new Gson().fromJson(json_string, Map.class);
        return new ContainerConfiguration(configs);
    }
}

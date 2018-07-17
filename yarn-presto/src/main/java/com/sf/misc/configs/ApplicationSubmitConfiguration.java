package com.sf.misc.configs;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import javafx.application.Application;
import scala.App;

import java.nio.charset.Charset;
import java.util.Base64;
import java.util.Map;

public class ApplicationSubmitConfiguration {

    protected static final Charset CHARSET = Charset.availableCharsets().get("UTF-8");
    protected static final String MASTER_KEY = "_master_class_";


    protected final Map<String, String> configuraiton;

    public String getMaster() {
        return configuraiton.get(MASTER_KEY);
    }

    public ApplicationSubmitConfiguration(Class<?> master) {
        this(Maps.newConcurrentMap());
        this.configuraiton.put(MASTER_KEY, master.getName());
    }

    protected ApplicationSubmitConfiguration(Map<String, String> configuraiton) {
        this.configuraiton = configuraiton;
    }

    public Map<String, String> configs() {
        return ImmutableMap.<String, String>builder().putAll(this.configuraiton).build();
    }

    public ApplicationSubmitConfiguration addAirliftStyleConfig(Object object) {
        this.configuraiton.put(object.getClass().getName(), new Gson().toJson(object));
        return this;
    }

    public <T> T distill(Class<T> clazz) {
        return new Gson().fromJson(this.configuraiton.get(clazz.getName()), clazz);
    }

    public static String embedded(ApplicationSubmitConfiguration configuration) {
        // json
        String json = new Gson().toJson(configuration.configs());

        return Base64.getEncoder().encodeToString(json.getBytes(CHARSET));
    }

    public static ApplicationSubmitConfiguration recover(String base64_json) {
        byte[] json = Base64.getDecoder().decode(base64_json.getBytes(CHARSET));
        String json_string = new String(json, CHARSET);

        Map<String, String> configs = new Gson().fromJson(json_string, Map.class);
        return new ApplicationSubmitConfiguration(configs);
    }
}

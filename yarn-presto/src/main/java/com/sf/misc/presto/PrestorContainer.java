package com.sf.misc.presto;

import com.facebook.presto.server.PrestoServer;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Module;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;

public class PrestorContainer {

    public static void main(String args[]) {
        // prepare config.json
        File config = new File("config.json");

        // generate config
        try (FileOutputStream stream = new FileOutputStream(config)) {
            Properties properties = new Properties(System.getProperties());

            properties.store(stream, "presto generated config file");
        } catch (IOException e) {
            throw new RuntimeException("fail to create config", e);
        }

        // update properties
        System.setProperty("config", config.getAbsolutePath());

        new PrestoServer() {
            protected Iterable<? extends Module> getAdditionalModules() {
                return ImmutableList.of();
            }
        }.run();
    }
}

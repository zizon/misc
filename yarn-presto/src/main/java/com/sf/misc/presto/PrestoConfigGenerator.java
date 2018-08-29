package com.sf.misc.presto;

import com.facebook.presto.server.ServerConfig;
import com.facebook.presto.server.ServerMainModule;
import com.sf.misc.airlift.AirliftConfig;
import com.sf.misc.airlift.AirliftPropertyTranscript;
import com.sf.misc.async.ListenablePromise;
import com.sf.misc.async.Promises;
import com.sf.misc.yarn.ContainerConfiguration;
import com.sf.misc.yarn.launcher.ContainerLauncher;
import com.sf.misc.yarn.launcher.LauncherEnviroment;
import io.airlift.log.Logger;
import org.apache.hadoop.yarn.api.ApplicationConstants;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;
import java.util.Properties;

public class PrestoConfigGenerator {

    public static final Logger LOGGER = Logger.get(PrestoConfigGenerator.class);

    public static final File INSTALLED_PLUGIN_DIR = new File("plugin");
    public static final File CATALOG_CONFIG_DIR = new File("etc/catalog/");
    public static final File PASSWORD_AUTHENTICATOR_CONFIG = new File("etc/password-authenticator.properties");
    public static final File ACCESS_CONTORL_CONFIG = new File("etc/access-control.properties");

    public static ListenablePromise<File> generatePrestoConfig(ContainerConfiguration container_context) {
        return Promises.submit(() -> {
            // prepare config.json
            File config = new File("config.json");

            try (FileOutputStream stream = new FileOutputStream(config)) {
                PrestoContainerConfig presto = container_context.distill(PrestoContainerConfig.class);

                Properties properties = new Properties();

                // add airlift config for federation moudle etc.
                properties.putAll(AirliftPropertyTranscript.toProperties(container_context.distill(AirliftConfig.class)));

                // coordiantor setting
                properties.put("discovery-server.enabled", Boolean.toString(presto.getCoordinator()));
                properties.put("coordinator", Boolean.toString(presto.getCoordinator()));

                // walkaround for http classloader that can not find version info from package
                properties.put("presto.version", "unknown");

                // dynamic port
                properties.put("http-server.http.port", "0");

                // agree dirs
                properties.put("plugin.dir", INSTALLED_PLUGIN_DIR.getAbsolutePath());
                properties.put("catalog.config-dir", CATALOG_CONFIG_DIR.getAbsolutePath());

                // use current dir as log dir
                File log_dir = new File(LauncherEnviroment.logdir());
                properties.put("log.enable-console", "false");
                properties.put("log.path", new File(log_dir, "presto.log").getAbsolutePath());
                properties.put("http-server.log.path", new File(log_dir, "http-request.log").getAbsolutePath());

                // log levels
                File log_level = new File(log_dir, "log-levles.properties");
                try (FileOutputStream log_level_stream = new FileOutputStream(log_level)) {
                    Properties log_properties = new Properties();
                    log_properties.putAll(container_context.logLevels());
                    log_properties.store(log_level_stream, "log levels");
                }
                properties.put("log.levels-file", log_level.getAbsolutePath());

                properties.store(stream, "presto generated config file");
            }

            return config;
        });
    }

    public static PluginBuilder newPluginBuilder(URI classloader) {
        return new PluginBuilder(INSTALLED_PLUGIN_DIR, classloader);
    }
}

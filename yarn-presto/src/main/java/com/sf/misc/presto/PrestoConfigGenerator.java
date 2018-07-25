package com.sf.misc.presto;

import com.facebook.presto.server.ServerMainModule;
import com.sf.misc.airlift.AirliftConfig;
import com.sf.misc.async.ListenablePromise;
import com.sf.misc.async.Promises;
import com.sf.misc.yarn.ContainerConfiguration;
import com.sf.misc.yarn.launcher.ContainerLauncher;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;
import java.util.Properties;

public class PrestoConfigGenerator {

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
                AirliftConfig airlift = container_context.distill(AirliftConfig.class);

                Properties properties = new Properties();
                properties.put("coordinator", Boolean.toString(presto.getCoordinator()));
                properties.put("presto.version", ServerMainModule.class.getPackage().getImplementationVersion());
                properties.put("service-inventory.uri", airlift.getInventory());
                properties.put("node.environment", airlift.getNodeEnv());
                properties.put("http-server.http.port", "0");

                properties.put("plugin.dir", INSTALLED_PLUGIN_DIR.getAbsolutePath());
                properties.put("catalog.config-dir", CATALOG_CONFIG_DIR.getAbsolutePath());

                File log_dir = new File(System.getenv().get(ContainerLauncher.Enviroments.CONTAINER_LOG_DIR.name()));
                properties.put("log.enable-console", "false");
                properties.put("log.path", new File(log_dir, "presto.log").getAbsolutePath());
                properties.put("http-server.log.path", new File(log_dir, "http-request.log").getAbsolutePath());

                properties.store(stream, "presto generated config file");
            }

            return config;
        });
    }

    public static PluginBuilder newPluginBuilder(URI classloader) {
        return new PluginBuilder(INSTALLED_PLUGIN_DIR, classloader);
    }
}

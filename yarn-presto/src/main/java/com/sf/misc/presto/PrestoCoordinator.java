package com.sf.misc.presto;

import com.facebook.presto.hive.HiveHadoop2Plugin;
import com.facebook.presto.server.ServerMainModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.sf.misc.async.ListenablePromise;
import com.sf.misc.async.Promises;
import com.sf.misc.configs.ApplicationSubmitConfiguration;
import com.sf.misc.yarn.ConfigurationGenerator;
import com.sf.misc.yarn.ContainerLauncher;
import com.sf.misc.yarn.YarnRMProtocolConfig;
import io.airlift.log.Logger;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Properties;

public class PrestoCoordinator {

    public static final Logger LOGGER = Logger.get(PrestoCoordinator.class);


    protected static File INSTALLED_PLUGIN_DIR = new File("plugin");
    protected static File CATALOG_CONFIG_DIR = new File("etc/catalog/");
    protected static File PASSWORD_AUTHENTICATOR_CONFIG = new File("etc/password-authenticator.properties");
    protected static File ACCESS_CONTORL_CONFIG = new File("etc/access-control.properties");

    public static void main(String args[]) throws Throwable {
        LOGGER.info("start up...");
        ApplicationSubmitConfiguration configuration = ApplicationSubmitConfiguration.recover(System.getenv().get(ApplicationSubmitConfiguration.class.getName()));
        configuration.configs().entrySet()
                .parallelStream()
                .forEach((entry) -> {
                    LOGGER.info("context config key:" + entry.getKey() + " value:" + entry.getValue());
                });


        LOGGER.info(configuration.distill(YarnRMProtocolConfig.class).getRMs());

        // prepare config.json
        ListenablePromise<File> config = generatePrestorCoreConfiguration(true).transform((config_file) -> {
            // update properties
            System.setProperty("config", config_file.getAbsolutePath());
            return config_file;
        });

        // core-site.xml
        ListenablePromise<File> hdfs_config = generateHdfsConfiguration(configuration);


    }

    protected static ListenablePromise<File> generateHdfsConfiguration(ApplicationSubmitConfiguration configuration) {
        return Promises.submit(() -> {
            File config_file = new File(CATALOG_CONFIG_DIR, "core-site.xml");

            // prepare xml
            Configuration hdfs_xml = new Configuration();
            ConfigurationGenerator generator = new ConfigurationGenerator();
            Arrays.stream( //
                    configuration.distill(HiveServicesConfig.class) //
                            .getNameservives() //
                            .split(";") //
            ).map((nameservice) -> {
                return generator.generateHdfsHAConfiguration(URI.create(nameservice));
            }).flatMap((map) -> {
                return map.entrySet().parallelStream();
            }).parallel().forEach((entry) -> {
                hdfs_xml.set(entry.getKey(), entry.getValue());
            });

            // write out
            try (FileOutputStream stream = new FileOutputStream(config_file)) {
                hdfs_xml.writeXml(stream);
            }

            return config_file;
        });
    }

/*
    protected static ListenablePromise<Throwable> setupHive(PluginBuilder builder) {
        ListenablePromise<Throwable> plugin_ready = builder.setupPlugin(HiveHadoop2Plugin.class);

        File catalog_config = new File(CATALOG_CONFIG_DIR, "hive.properties");
        File hadoop_config = new File(CATALOG_CONFIG_DIR, "core-site.xml");

        ListenablePromise<Throwable> hdfs_config_ready = builder.ensureDirectory(hadoop_config).transform((throwable) -> {
            if (throwable != null) {
                return throwable;
            }
            LOGGER.info("prepare hadoop config:" + hadoop_config);

            Configuration configuration = new Configuration();
            system_properties.entrySet().parallelStream() //
                    .filter((entry) -> entry.getKey().toString().startsWith(PrestoProperties.HdfsConfig.prefix())) //
                    .sequential() //
                    .forEach((entry) -> {
                        String key = entry.getKey().toString().substring(PrestoProperties.HdfsConfig.prefix().length() + 1);
                        String value = entry.getValue().toString();
                        configuration.set(key, value);
                    });

            // persist
            try (FileOutputStream stream = new FileOutputStream(hadoop_config)) {
                configuration.writeXml(stream);
            } catch (IOException exception) {
                return exception;
            }

            return null;
        });

        ListenablePromise<Throwable> hive_config_ready = builder.ensureDirectory(catalog_config).transform((throwable) -> {
            if (throwable != null) {
                return throwable;
            }

            LOGGER.info("prepare catalog config:" + catalog_config);
            try (FileOutputStream stream = new FileOutputStream(catalog_config)) {
                Properties properties = new Properties();

                // setup connetor name
                properties.put("connector.name", "hive-hadoop2");

                // set metastore type
                properties.put("hive.metastore", "thrift");

                properties.put("hive.compression-codec", "SNAPPY");

                properties.put("hive.config.resources", hadoop_config.getAbsolutePath());

                //properties.put("hive.hdfs.impersonation.enabled", "true");

                // copy config
                Arrays.asList(
                        "hive.metastore.uri"
                ).forEach((key) -> {
                    properties.put(key, system_properties.getProperty(getHiveCatalogConfigKey(key)));
                });

                properties.store(stream, "hive catalog config");
            } catch (IOException exception) {
                return exception;
            }

            return null;
        });

        return ImmutableList.of(plugin_ready, hdfs_config_ready, hive_config_ready).stream().reduce((left, right) -> {
            return left.transformAsync((throwable) -> {
                if (throwable != null) {
                    return left;
                }
                return right;
            });
        }).orElse(null);
    }
*/
    protected static ListenablePromise<File> generatePrestorCoreConfiguration(boolean coordinator) {
        return Promises.submit(() -> {
            // prepare config.json
            File config = new File("config.json");

            // generate config
            try (FileOutputStream stream = new FileOutputStream(config)) {
                Properties properties = new Properties();
                properties.putAll( //
                        new ImmutableMap.Builder<String, String>() //
                                .put("coordinator", Boolean.toString(coordinator))
                                .put("presto.version", ServerMainModule.class.getPackage().getImplementationVersion()) //
                                //.put("discovery.uri", server_info.getHttpUri().toString()) //
                                //.put("service-inventory.uri", server_info.getHttpUri().toString() + "/v1/service") //
                                .put("node.environment", "yarn-presto") //
                                .put("http-server.http.port", "0") //
                                .build() //
                );


                properties.put("plugin.dir", INSTALLED_PLUGIN_DIR.getAbsolutePath());
                properties.put("catalog.config-dir", CATALOG_CONFIG_DIR.getAbsolutePath());

                File log_dir = new File(System.getenv().get(ContainerLauncher.Enviroments.CONTAINER_LOG_DIR.name()));
                properties.put("log.enable-console", "false");
                properties.put("log.path", new File(log_dir, "presto.log").getAbsolutePath());
                properties.put("http-server.log.path", new File(log_dir, "http-request.log").getAbsolutePath());

                //properties.put("http-server.authentication.type", "PASSWORD");

                properties.store(stream, "presto generated config file");
            }

            return config;
        });
    }
}

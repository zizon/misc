package com.sf.misc.presto.plugins.hive;

import com.facebook.presto.hive.HiveHadoop2Plugin;
import com.sf.misc.async.ListenablePromise;
import com.sf.misc.presto.PluginBuilder;
import com.sf.misc.presto.PrestoConfigGenerator;
import com.sf.misc.presto.plugins.PluginInstaller;
import com.sf.misc.yarn.ContainerConfiguration;
import com.sf.misc.yarn.launcher.ConfigurationGenerator;
import io.airlift.log.Logger;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

public class HivePluginInstaller implements PluginInstaller {

    public static final Logger LOGGER = Logger.get(HivePluginInstaller.class);

    protected final PluginBuilder builder;
    protected final ContainerConfiguration configuration;

    public HivePluginInstaller(PluginBuilder builder, ContainerConfiguration configuration) {
        this.builder = builder;
        this.configuration = configuration;
    }

    @Override
    public Collection<ListenablePromise<File>> install() {
        HiveServicesConfig rm_config = configuration.distill(HiveServicesConfig.class);

        ListenablePromise<File> core_site = generateCoreSite();
        ListenablePromise<File> hive_properties = generateHiveProperties();
        ListenablePromise<File> plugin_jar = builder.install(HiveHadoop2Plugin.class);

        return Arrays.asList(core_site, hive_properties);
    }


    protected ListenablePromise<File> generateHiveProperties() {
        return builder.ensureDirectory(new File(PrestoConfigGenerator.CATALOG_CONFIG_DIR, "hive.properties")) //
                .transform((config_file) -> {
                    LOGGER.info("prepare hive.properties config:" + config_file);

                    try (FileOutputStream stream = new FileOutputStream(config_file)) {
                        Properties properties = new Properties();

                        // setup connetor name
                        properties.put("connector.name", "hive-hadoop2");

                        // set metastore type
                        properties.put("hive.metastore", "thrift");

                        properties.put("hive.compression-codec", "SNAPPY");

                        properties.put("hive.config.resources", coreSiteFile().getAbsolutePath());

                        properties.put("hive.metastore.uri", configuration.distill(HiveServicesConfig.class).getMetastore());

                        //properties.put("hive.hdfs.impersonation.enabled", "true");
                        properties.store(stream, "hive catalog config");
                    }

                    return config_file;
                });
    }

    protected File coreSiteFile() {
        return new File(PrestoConfigGenerator.CATALOG_CONFIG_DIR, "core-site.xml");
    }

    protected ListenablePromise<File> generateCoreSite() {
        return builder.ensureDirectory(coreSiteFile()) //
                .transform((config_file) -> {
                    LOGGER.info("prepare core-site config:" + config_file);

                    Configuration core_config = new Configuration();

                    new ConfigurationGenerator()
                            .generateHdfsHAConfiguration( //
                                    URI.create( //
                                            configuration.distill(HiveServicesConfig.class) //
                                                    .getNameservives()
                                    ) //
                            )
                            .entrySet()
                            .forEach((entry) -> {
                                core_config.set(entry.getKey(), entry.getValue());
                            });
                    // persist
                    try (FileOutputStream stream = new FileOutputStream(config_file)) {
                        core_config.writeXml(stream);
                    }

                    return config_file;
                });
    }
}

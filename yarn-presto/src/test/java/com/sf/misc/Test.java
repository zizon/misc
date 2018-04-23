package com.sf.misc;

import com.facebook.presto.hive.HiveHadoop2Plugin;
import com.facebook.presto.spi.Plugin;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.sf.misc.async.ExecutorServices;
import com.sf.misc.classloaders.JarCreator;
import org.junit.Assert;
import sun.plugin.security.PluginClassLoader;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Test {
    private static final ImmutableList<String> SPI_PACKAGES = ImmutableList.<String>builder()
            .add("com.facebook.presto.spi.")
            .add("com.fasterxml.jackson.annotation.")
            .add("io.airlift.slice.")
            .add("io.airlift.units.")
            .add("org.openjdk.jol.")
            .build();

    @org.junit.Test
    public void testCollections() throws Exception {
        Set<Integer> ints = Sets.newConcurrentHashSet();
        for (int i = 0; i < 10000; i++) {
            ints.add(i);
        }

        ints.parallelStream().map(Function.identity()).map((i)->{
            if (i%2==0){
                ints.remove(i);
                return null;
            }else {
                return i;
            }
        }).count();

        System.out.println(ints.size());
    }

    @org.junit.Test
    public void test() throws MalformedURLException {
        Pattern pattern = Pattern.compile(
                "^[^\\s]+\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s[^\\s]+\\s[^\\s]+$");
        String line = "cgroup /sys/fs/cgroup/cpu,cpuacct cgroup rw,nosuid,nodev,noexec,relatime,cpuacct,cpu 0 0\n";
        Matcher matcher = pattern.matcher(line);
        Assert.assertTrue(matcher.find());
        for (int i = 1; i <= 3; i++) {
            System.out.println(matcher.group(i));
        }

        System.out.println(new File(".").toURI().toURL());
        System.out.println(HiveHadoop2Plugin.class.getName());

        List<URL> urls = new ArrayList<>();

        // setup plugin
        Arrays.asList(
                HiveHadoop2Plugin.class
        ).stream().forEach((clazz) -> {
            String path = "./plugin/" + clazz.getSimpleName() + "/META-INF/services";

            File services = new File(path);

            /*
            ((ExecutorServices.Lambda) () -> {
                for (File file : listFiles(services)) {
                    urls.add(file.toURI().toURL());
                }
            }).run();
            */
            if (!services.exists() && !services.mkdirs()) {
                throw new RuntimeException("path not found:" + path);
            }

            try (FileOutputStream stream = new FileOutputStream(new File(path + "/" + "com.facebook.presto.spi.Plugin"))) {
                stream.write(clazz.getName().toString().getBytes());
            } catch (IOException exception) {
                throw new RuntimeException("fail to write service porvider info", exception);
            }
            /*
            try (FileOutputStream stream = new FileOutputStream(new File(path + "/" + "bundle.jar"))) {
                new JarCreator() //
                        .add("META-INF/services/com.facebook.presto.spi.Plugin", () -> ByteBuffer.wrap(clazz.getName().toString().getBytes())) //
                        .write(stream);
                ;
            } catch (IOException exception) {
                throw new RuntimeException("fail to write service porvider info", exception);
            }
            */
        });

        urls.add(new File("./plugin/" + HiveHadoop2Plugin.class.getSimpleName()).toURI().toURL());
        /*
        for (File file : listFiles(new File("./plugin/" + HiveHadoop2Plugin.class.getSimpleName()))) {
            urls.add(file.toURI().toURL());
        }
        */
        System.out.println(urls);
        Assert.assertTrue(ServiceLoader.load(Plugin.class, createClassLoader(urls)).iterator().hasNext());
    }

    private static URLClassLoader createClassLoader(List<URL> urls) {
        return new URLClassLoader(urls.toArray(new URL[0]), null);
    }

    private static List<File> listFiles(File installedPluginsDir) {
        if (installedPluginsDir != null && installedPluginsDir.isDirectory()) {
            File[] files = installedPluginsDir.listFiles();
            if (files != null) {
                Arrays.sort(files);
                return ImmutableList.copyOf(files);
            }
        }
        return ImmutableList.of();
    }
}

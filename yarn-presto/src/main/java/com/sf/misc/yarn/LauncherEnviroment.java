package com.sf.misc.yarn;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.sf.misc.async.ListenablePromise;
import com.sf.misc.async.Promises;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Resource;

import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public class LauncherEnviroment {

    protected final URI classloader;

    public LauncherEnviroment(URI classloader) {
        this.classloader = classloader;
    }

    public ImmutableMap<String, String> enviroments() {
        return ImmutableMap.<String, String>builder() //
                .put(ApplicationConstants.Environment.CLASSPATH.key(), ".:./*") //
                //.put(ContainerLauncher.Enviroments.CONTAINER_LOG_DIR.name(), ApplicationConstants.LOG_DIR_EXPANSION_VAR) //
                .build();
    }

    public ListenablePromise<ImmutableList<String>> launcherCommand(Resource resource, Map<String, String> properties, Class<?> entry_class) {
        return Promises.submit(() -> {
            // launch command
            ImmutableList.Builder<String> commands = ImmutableList.builder();

            // for debug
            commands.addAll(debugCommand());
            commands.addAll(prepareJar(classloader.toURL().toExternalForm()));

            // java
            commands.add(ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java");

            // heap tuning
            commands.add("-Xmx" + resource.getMemory() + "M");
            if (resource.getMemory() > 5 * 1024) {
                commands.add("-XX:+UseG1GC");
            } else {
                commands.add("-XX:+UseParallelGC");
                commands.add("-XX:+UseParallelOldGC");
            }

            // gc log
            Arrays.asList("-XX:+PrintGCDateStamps", //
                    "-verbose:gc", //
                    "-XX:+PrintGCDetails", //
                    "-Xloggc:" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/gc.log", //
                    "-XX:+UseGCLogFileRotation", //
                    "-XX:NumberOfGCLogFiles=10", //
                    "-XX:GCLogFileSize=8M" //
            ).stream().sequential().forEach((command) -> commands.add(command));

            // pass properties
            Optional.ofNullable(properties).orElse(Collections.emptyMap()).entrySet().stream().sequential() //
                    .forEach((entry) -> {
                        commands.add("-D" + entry.getKey() + "=" + entry.getValue());
                    });

            // entry class
            commands.add(KickStart.class.getName());

            // escape inner class with A$B.class to A\\\$B.class
            commands.add(entry_class.getName().replace("$", "\\\\\\$"));

            // logs
            commands.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/" + ApplicationConstants.STDOUT);
            commands.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/" + ApplicationConstants.STDERR);

            commands.add(";\n");
            return commands.build();
        });
    }

    protected List<String> debugCommand() {
        // launch command
        ImmutableList.Builder<String> commands = ImmutableList.builder();

        // for debug
        commands.addAll(Arrays.asList("rm", "-rf", "/tmp/scripts\n"));
        commands.addAll(Arrays.asList("mkdir", "-p", "/tmp/scripts;\n"));
        commands.addAll(Arrays.asList("cp", "-r", ".", "/tmp/scripts/;\n"));

        return commands.build();
    }

    protected List<String> prepareJar(String http_classloader) {
        List<String> command = Lists.newLinkedList();

        String meta_inf = "META-INF";
        String manifest = "MANIFEST.MF";

        // prepare jar dir
        command.addAll(Arrays.asList("mkdir", "-p", meta_inf));
        command.add(";\n");

        // touch
        command.addAll(Arrays.asList("touch", meta_inf + "/" + manifest));
        command.add(";\n");

        // write
        command.addAll(Arrays.asList("echo", "'Manifest-Version: 1.0'", ">" + meta_inf + "/" + manifest));
        command.add(";\n");

        command.addAll(Arrays.asList("echo", "'Class-Path: " + http_classloader + "'", ">>" + meta_inf + "/" + manifest));
        command.add(";\n");

        // zip
        command.addAll(Arrays.asList("zip", "-r", "_kickstart_" + UUID.randomUUID().toString() + ".jar", meta_inf));
        command.add(";\n");

        command.addAll(Arrays.asList("ls", "-l", "-a", "."));
        command.add(";\n");
        return command;
    }
}

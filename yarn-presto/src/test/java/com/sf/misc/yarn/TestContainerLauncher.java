package com.sf.misc.yarn;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class TestContainerLauncher {

    @Test
    public void test() throws Exception {
        ContainerLaunch.ShellScriptBuilder sb = ContainerLaunch.ShellScriptBuilder.create();
        List<String> commands = Lists.newLinkedList();
        commands.add(ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java");
        commands.add(KickStart.class.getName());

        sb.env(ApplicationConstants.Environment.CLASSPATH.key(), ".");
        sb.command(commands);
        System.out.println(sb.toString());
    }
}

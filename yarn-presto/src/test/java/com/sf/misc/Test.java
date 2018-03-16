package com.sf.misc;

import org.junit.Assert;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Test {

    @org.junit.Test
    public void test() {
        Pattern pattern = Pattern.compile(
                "^[^\\s]+\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s[^\\s]+\\s[^\\s]+$");
        String line = "cgroup /sys/fs/cgroup/cpu,cpuacct cgroup rw,nosuid,nodev,noexec,relatime,cpuacct,cpu 0 0\n";
        Matcher matcher = pattern.matcher(line);
        Assert.assertTrue(matcher.find());
        for (int i = 1; i <= 3; i++) {
            System.out.println(matcher.group(i));
        }
    }
}

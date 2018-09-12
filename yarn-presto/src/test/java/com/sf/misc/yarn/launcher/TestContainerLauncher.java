package com.sf.misc.yarn.launcher;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import io.airlift.log.Logger;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.junit.Test;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TestContainerLauncher {

    public static final Logger LOGGER = Logger.get(TestContainerLauncher.class);

    @Test
    public void test() {
        List<Resource> resources = Lists.newArrayList();
        resources.addAll(IntStream.range(0, 10).mapToObj((i) -> Resource.newInstance(1, 1)).collect(Collectors.toList()));
        resources.addAll(IntStream.range(0, 10).mapToObj((i) -> Resource.newInstance(2, 2)).collect(Collectors.toList()));

        resources.parallelStream()
                .collect(Collectors.groupingByConcurrent(Function.identity()))
                .entrySet()
                .parallelStream()
                .map((resource_group) -> {
                    return ResourceRequest.newInstance(
                            Priority.UNDEFINED,
                            ResourceRequest.ANY,
                            resource_group.getKey(),
                            resource_group.getValue().size()
                    );
                }).collect(Collectors.toList())
                .forEach((item) -> {
                    LOGGER.info("item:" + item);
                });
    }
}

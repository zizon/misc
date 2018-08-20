package com.sf.misc;

import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import io.airlift.log.Logger;
import org.junit.Test;

import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class TestWork {

    public static final Logger LOGGER = Logger.get(TestWork.class);

    @Test
    public void test() {
        ConcurrentMap<String, String> map = Maps.newConcurrentMap();
        IntStream.range(0, 100).forEach((x) -> {
            map.put("" + x, "" + x);
        });

        LOGGER.info("size:" + map.size());
        Iterators.consumingIterator(map.entrySet().iterator()).forEachRemaining((x)->{});

        LOGGER.info("size:" + map.size());
    }
}

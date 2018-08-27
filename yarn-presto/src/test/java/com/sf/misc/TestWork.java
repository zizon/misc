package com.sf.misc;

import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.sf.misc.airlift.Airlift;
import com.sf.misc.airlift.AirliftConfig;
import com.sf.misc.airlift.AirliftPropertyTranscript;
import com.sf.misc.yarn.ContainerConfiguration;
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
        String a="eyJjb20uc2YubWlzYy5wcmVzdG8uUHJlc3RvQ29udGFpbmVyQ29uZmlnIjoie1wiY29vcmRpbmF0b3JcIjpmYWxzZSxcIm1lbW9yeVwiOjUxMn0iLCJfbWFzdGVyX2NsYXNzXyI6ImNvbS5zZi5taXNjLnByZXN0by5QcmVzdG9Db250YWluZXIiLCJfQ0xBU1NMT0FERVJfIjoiaHR0cDovLzEwLjExOC4xNS40MTo4MDgwL3YxL2h0dHAtY2xhc3Nsb2FkZXIvIiwiY29tLnNmLm1pc2MucHJlc3RvLnBsdWdpbnMuaGl2ZS5IaXZlU2VydmljZXNDb25maWciOiJ7XCJuYW1lc2Vydml2ZXNcIjpcInRlc3QtY2x1c3RlcjovLzEwLjIwMi43Ny4yMDA6ODAyMCwxMC4yMDIuNzcuMjAxOjgwMjBcIixcIm1ldGFzdG9yZVwiOlwidGhyaWZ0Oi8vMTAuMjAyLjc3LjIwMDo5MDgzXCJ9IiwiY29tLnNmLm1pc2MuYWlybGlmdC5BaXJsaWZ0Q29uZmlnIjoie1wibm9kZV9lbnZcIjpcInRlc3RcIixcInBvcnRcIjowLFwiY2xhc3Nsb2FkZXJcIjpcImh0dHA6Ly8xMC4xMTguMTUuNDE6ODA4MC92MS9odHRwLWNsYXNzbG9hZGVyL1wiLFwibG9nbGV2ZWxcIjpcImFpcmxpZnQtbG9nLmNvbmZpZ1wifSIsIl9DUFVfIjoiMSIsIl9NRU1PUllfIjoiNTEyIn0=";
        ContainerConfiguration configuration = ContainerConfiguration.decode(a);
        LOGGER.info(new Gson().toJson(configuration.configs()));
    }

    @Test
    public void testTranscript(){
        AirliftConfig config  = new AirliftConfig();
        config.setDiscovery("some");

        LOGGER.info(""+ AirliftPropertyTranscript.toProperties(config));
    }
}

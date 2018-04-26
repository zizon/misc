package com.sf.misc.presto;

import com.facebook.presto.client.ClientSession;
import com.facebook.presto.client.QueryData;
import com.facebook.presto.client.StatementClient;
import com.facebook.presto.client.StatementClientFactory;
import com.sf.misc.airlift.Airlift;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceInventory;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import okhttp3.OkHttpClient;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

public class TestPrestoStatement {
    public static final Logger LOGGER = Logger.get(TestPrestoStatement.class);

    Airlift airlift;
    String app_name = "yarn-presto-container";

    @Before
    public void setupClient() throws Exception {
        Map<String, String> configuration = new HashMap<>();

        configuration.put("service-inventory.uri", "http://10.110.100.101:8080/v1/service");
        configuration.put("discovery.uri", "http://10.110.100.101:8080");
        configuration.put("node.environment", "production");

        configuration.put("log.levels-file",  //
                new File(Thread.currentThread().getContextClassLoader() //
                        .getResource("airlift-log.properties") //
                        .toURI() //
                ).getAbsolutePath() //
        );

        airlift = new Airlift().withConfiguration(configuration) //
                .start();
    }

    @Test
    public void test() throws Exception {
        ServiceInventory inventory = airlift.getInstance(ServiceInventory.class);
        URL server = null;
        for (; ; ) {
            Iterator<ServiceDescriptor> iterator = inventory.getServiceDescriptors("presto-coordinator").iterator();
            if (iterator.hasNext()) {
                ServiceDescriptor descriptor = iterator.next();
                server = new URL(descriptor.getProperties().get("http"));
                break;
            }

            LOGGER.info("wait a little...");
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(5));
        }

        OkHttpClient okhttp = new OkHttpClient.Builder().readTimeout(10, TimeUnit.SECONDS).build();
        LOGGER.info("start a session");
        ClientSession session = new ClientSession(
                server.toURI(), //
                "hive", //
                "generated-client-session", //
                Collections.emptySet(),
                "",
                "hive",
                "",
                TimeZone.getDefault().getID(),
                Locale.getDefault(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                "",
                new Duration(60, TimeUnit.SECONDS)
        );

        LOGGER.info("build a statement");
        String query = "select * FROM dm_fin_vcs.vcs_fact_cust_oper_gross limit 1000";
        StatementClient statement = StatementClientFactory.newStatementClient(okhttp, session, query);

        /*
        LOGGER.info("statement:" + statement.isRunning());
        while (statement.advance()){
            LOGGER.info(statement.currentData().toString());
        }
        */
        new Iterator<QueryData>() {
            @Override
            public boolean hasNext() {
                return statement.advance();
            }

            @Override
            public QueryData next() {
                return statement.currentData();
            }
        }.forEachRemaining((data) -> {
            LOGGER.info("got a batch data:....");
            data.getData().forEach((row) -> {
                LOGGER.info("row:" + row.stream() //
                        .map(Optional::ofNullable) //
                        .map((optional) -> optional.orElse("NULL_VALUE").toString()) //
                        .collect(Collectors.joining(",")));
            });
        });
    }
}

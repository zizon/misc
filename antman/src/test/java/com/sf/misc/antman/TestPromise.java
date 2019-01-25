package com.sf.misc.antman;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.ParameterizedType;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class TestPromise {

    public static final Log LOGGER = LogFactory.getLog(TestPromise.class);

    @Test
    public void testPriod() {
        Promise<?> scheudle = Promise.period(() -> {
            LOGGER.info("run once");
        }, 3000, (throwable) -> {
            LOGGER.error("period fail with exception", throwable);
        });
        LOGGER.info("schedule:" + scheudle);

        Promise.delay(() -> {
            LOGGER.info("cancle schedule:" + scheudle.cancel(true) + " scheudle:" + scheudle);
        }, 5 * 1000, (throwable) -> {
            LOGGER.error("delay cancle fail with exception", throwable);
        });

        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(20));
        LOGGER.info("done");
    }

    @Test
    public void testPeriodExceptional() {
        Promise<?> exceptional = Promise.period(() -> {
            LOGGER.info("exception run");
            throw new RuntimeException("fail intented");
        }, 3000, (throwable) -> {
            LOGGER.error("period fail with exception", throwable);
        });

        LOGGER.info("exceptional:" + exceptional);
        Promise.delay(() -> {
            LOGGER.info("cancel exceptoin");
            exceptional.cancel(true);
        }, 10 * 1000, null);

        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(20));
        LOGGER.info("done");
    }

    @Test
    public void testDeduce() {
        Promise<?> t = new Promise<Promise<Boolean>>();

        Arrays.stream(t.getClass().getGenericInterfaces()).forEach((type) -> {
            //sun.reflect.generics.reflectiveObjects.TypeVariableImpl
            ParameterizedType generic = (ParameterizedType) type;
            LOGGER.info(type);
            LOGGER.info(type instanceof ParameterizedType);
            Arrays.stream(generic.getActualTypeArguments()).forEach((actual) -> {

            });

            //LOGGER.info( type.getBounds()[0]);

        });
    }

    @Test
    public void testTimeout() {
        Promise<Boolean> tiemout_value = Promise.promise();

        // will execute after 10 seconds
        Promise.delay(() -> {
            tiemout_value.complete(true);
        }, TimeUnit.SECONDS.toMillis(10));

        // 2 sencod timeout
        Promise<Boolean> final_value = tiemout_value.timeout(() -> false, TimeUnit.SECONDS.toMillis(2));

        // will get timeout value
        Assert.assertFalse(final_value.join());

        // will execute in 5 seconds
        Promise<Boolean> not_timeout_value = Promise.promise();
        Promise.delay(() -> {
            not_timeout_value.complete(true);
        }, TimeUnit.SECONDS.toMillis(2));

        // 5 sencod timeout
        final_value = not_timeout_value.timeout(() -> false, TimeUnit.SECONDS.toMillis(5));

        // will get normal value
        Assert.assertTrue(final_value.join());
    }

    @Test
    public void testDate() {
        String test = "2018年10-91 10:15:45";
        try {
            NumberFormat format = NumberFormat.getIntegerInstance(Locale.getDefault());
            //LOGGER.info(format.parse("年1"));

            LOGGER.info(new SimpleDateFormat("yyyyMM-dd").parse(test));

        } catch (ParseException e) {
            e.printStackTrace();
        }

    }
}

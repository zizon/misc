package com.sf.misc.antman;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import java.lang.reflect.ParameterizedType;
import java.util.Arrays;
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
    public void testDeduce(){
        Promise<?> t= new Promise<Promise<Boolean>>();

        Arrays.stream(t.getClass().getGenericInterfaces()).forEach((type)->{
           //sun.reflect.generics.reflectiveObjects.TypeVariableImpl
            ParameterizedType generic = (ParameterizedType) type;
            LOGGER.info(type);
            LOGGER.info(type instanceof ParameterizedType);
            Arrays.stream(generic.getActualTypeArguments()).forEach((actual)->{

            });

            //LOGGER.info( type.getBounds()[0]);

        });

    }
}

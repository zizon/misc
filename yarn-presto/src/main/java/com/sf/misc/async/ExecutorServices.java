package com.sf.misc.async;

import com.google.common.base.Functions;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.JdkFutureAdapters;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.airlift.log.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Arrays;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ExecutorServices {

    public static final Logger LOGGER = Logger.get(ExecutorServices.class);


    public static interface Lambda extends Runnable {
        public void apply() throws Exception;

        default public void run() {
            try {
                this.apply();
            } catch (Exception excetpion) {
                throw new RuntimeException("fail to apply", excetpion);
            }
        }
    }

    public static Lambda NOOP = () -> {
    };

    protected static ScheduledExecutorService SCHEDULE = Executors.newScheduledThreadPool(1);
    protected static ListeningExecutorService EXECUTOR = MoreExecutors.listeningDecorator(ForkJoinPool.commonPool());

    public static ListeningExecutorService executor() {
        return EXECUTOR;
    }

    public static ListenableFuture<Boolean> submit(Graph<Lambda> dag) {
        // setup condition
        ConcurrentMap<String, CountDownLatch> latches = dag.flip() //
                .vertexs().parallel() //
                .collect(Collectors.toConcurrentMap( //
                        Graph.Vertex::getName, //
                        (vertex) -> new CountDownLatch((int) vertex.outwardNames().count())) //
                );

        // then kick start
        return dag.vertexs().parallel() //
                .map((vertex) -> {
                    return executor().submit(() -> {
                        // wait for condition
                        latches.get(vertex.getName()).await();

                        // run
                        vertex.getPayload().orElse(() -> {
                            LOGGER.warn("vertext:" + vertex + " is not set properly");
                        }).run();

                        // then countdown
                        executor().execute(() -> {
                            vertex.outwardNames().parallel().forEach((outward) -> {
                                latches.get(outward).countDown();
                            });
                        });
                        return Boolean.TRUE;
                    });
                })
                .reduce((left, right) -> Futures.transformAsync(left, (ignore) -> right)
                ).get();
    }

    public static ListenableFuture<?> schedule(Lambda lambda, long period) {
        return JdkFutureAdapters.listenInPoolThread(SCHEDULE.scheduleAtFixedRate(lambda, 0, period, TimeUnit.MILLISECONDS));
    }
}

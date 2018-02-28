package com.sf.misc.async;

import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;

public class ExecutorServices {

    public static final Log LOGGER = LogFactory.getLog(ExecutorServices.class);


    public static interface Lambda extends Runnable {
        public void apply() throws Exception;

        default public void run() {
            try {
                this.apply();
            } catch (Exception excetpion) {
                LOGGER.error("fail to apply", excetpion);
            }
        }
    }

    public static Lambda NOOP = () -> {
    };

    protected static ListeningExecutorService EXECUTOR = MoreExecutors.listeningDecorator(ForkJoinPool.commonPool());

    public static ListeningExecutorService executor() {
        return EXECUTOR;
    }

    public static ListenableFuture<Boolean> submit(Graph<Lambda> dag) {
        // backlink
        Graph<Lambda> backlink = new Graph<>();
        dag.vertexs().parallel().forEach((vertex) -> {
            vertex.outwards().parallel().forEach((outward) -> {
                backlink.vertex(outward.getName()).link(vertex.getName());
            });
        });

        // setup barrier
        ConcurrentMap<String, CountDownLatch> latches = dag.vertexs().parallel().collect(ConcurrentHashMap::new, //
                (map, vertex) -> {
                    map.put(vertex.getName(), //
                            new CountDownLatch((int) backlink.vertex(vertex.getName()).outwards().count()));
                }, //
                ConcurrentHashMap::putAll //
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
                            vertex.outwards().parallel().forEach((outward) -> {
                                latches.get(outward.getName()).countDown();
                            });
                        });
                        return Boolean.TRUE;
                    });
                }).reduce((left, right) ->
                        Futures.transform(left, (AsyncFunction<Boolean, Boolean>) (ignore) -> right)
                ).get();
    }
}

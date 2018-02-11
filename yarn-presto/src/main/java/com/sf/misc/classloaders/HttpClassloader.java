package com.sf.misc.classloaders;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class HttpClassloader extends URLClassLoader implements AutoCloseable {

    private static final Log LOGGER = LogFactory.getLog(HttpClassloader.class);

    protected static ListeningExecutorService POOL = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());

    protected ConcurrentMap<Thread, ClassLoader> thread_stock_classloaders;
    protected AtomicInteger counter = new AtomicInteger(0);

    public HttpClassloader(URL[] urls, ClassLoader parent) {
        super(urls, parent);
        this.thread_stock_classloaders = new ConcurrentHashMap<>();
    }

    public HttpClassloader use() {
        Thread thread = Thread.currentThread();
        ClassLoader current_class_loader = Thread.currentThread().getContextClassLoader();

        if (thread_stock_classloaders.putIfAbsent(thread, current_class_loader) == null) {
            thread.setContextClassLoader(this);
            return this;
        }

        throw new IllegalStateException("thread already switch classloader,for thread:" + thread);
    }

    public void release() {
        Thread thread = Thread.currentThread();

        LOGGER.info("release:" + counter.incrementAndGet());
        ClassLoader loader = thread_stock_classloaders.remove(thread);
        if (loader != null) {
            LOGGER.info(loader);
            thread.setContextClassLoader(loader);
            return;
        }
        LOGGER.info("---------------------");
        throw new RuntimeException("no previrous class louder found,for thread:" + thread);
    }

    public boolean inContext() {
        return thread_stock_classloaders.get(Thread.currentThread()) != null;
    }

    @Override
    public void close() {
        //LOGGER.info("closed");
        this.release();
    }


    public static class Test {
        public class Test2 {
        }
    }

    private static HttpClassloader LOADER;

    static {
        try {
            LOADER = new HttpClassloader(new URL[]{new URL("http://localhost:8080/")}, null);
        } catch (MalformedURLException e) {
        }
    }

    protected static void onConnect() {
        String class_name = Test.Test2.class.getName();
        //class_name = AdminClient.class.getName();

        try (HttpClassloader loader = LOADER.use()) {
            // swap
            //LOGGER.info("start load...");
            //Class<?> clazz = LOADER.loadClass(class_name);
            Class<?> clazz = Class.forName(class_name);
            //LOGGER.info("load class:" + clazz);
        } catch (Exception e) {
            LOGGER.error("fail to load class:" + class_name, e);
        } finally {
            //LOGGER.info("load done?");
            LOADER.release();
            //throw new RuntimeException("???");
        }

        LOGGER.info("ok?");
    }

    public static void main(String[] args) {
        try {
            new HttpClassLoaderServer(new InetSocketAddress(8080)) {
                protected ChannelPipeline initializePipeline(ChannelPipeline pipeline) {
                    pipeline = super.initializePipeline(pipeline);
                    pipeline.addFirst(new LoggingHandler(LogLevel.INFO));
                    return pipeline;
                }
            }.bind()
                    .addListener((any) -> {
                        CountDownLatch latch = new CountDownLatch(100);

                        for (long i = latch.getCount(); i > 0; i--) {
                            POOL.submit(HttpClassloader::onConnect).addListener(() -> {
                                latch.countDown();
                                LOGGER.info(latch.getCount());
                            }, MoreExecutors.directExecutor());
                        }

                        POOL.execute(() -> {
                            try {
                                latch.await();
                            } catch (InterruptedException e) {
                            }
                            System.exit(0);
                        });
                    }).channel().closeFuture().get();
        } catch (Exception exception) {
            LOGGER.error("unexpected excetpion", exception);
        }

        LOGGER.info("----done");
    }
}

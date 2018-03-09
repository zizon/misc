package com.sf.misc.classloaders;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.sf.misc.async.ExecutorServices;
import io.airlift.log.Logger;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.LinkedList;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class HttpClassloader extends URLClassLoader implements AutoCloseable {

    protected ConcurrentMap<Thread, Stack<ClassLoader>> thread_stock_classloaders;

    public HttpClassloader(URL[] urls, ClassLoader parent) {
        super(urls, parent);
        this.thread_stock_classloaders = Maps.newConcurrentMap();

        ExecutorServices.schedule(() -> {
            thread_stock_classloaders.keySet().parallelStream() //
                    .filter(thread -> !thread.isAlive()) //
                    .forEach((thread) -> {
                        thread_stock_classloaders.remove(thread);
                    });
        }, TimeUnit.SECONDS.toMillis(5));
    }

    public HttpClassloader use() {
        Thread thread = Thread.currentThread();
        ClassLoader current_class_loader = thread.getContextClassLoader();

        thread_stock_classloaders.compute(thread, (key, old) -> {
            if (old == null) {
                old = new Stack<>();
            }

            old.push(current_class_loader);
            thread.setContextClassLoader(this);
            return old;
        });

        return this;
    }

    public void release() {
        Thread thread = Thread.currentThread();

        thread_stock_classloaders.compute(thread, (key, old) -> {
            // no check intented.
            // if old is null or empty , it should be a bug cause by
            // not calling use before
            thread.setContextClassLoader(old.pop());
            return old;
        });
    }

    public boolean inContext() {
        return !thread_stock_classloaders.compute(Thread.currentThread(), (key, old) -> {
            if (old == null) {
                old = new Stack<>();
            }

            return old;
        }).isEmpty();
    }

    @Override
    public void close() {
        this.release();
    }
}

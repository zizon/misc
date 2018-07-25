package com.sf.misc.async;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ListenablePromise<T> implements ListenableFuture<T> {

    public static final Logger LOGGER = Logger.get(ListenablePromise.class);

    protected final ListenableFuture<T> deleagte;

    public ListenablePromise(ListenableFuture<T> deleagte) {
        this.deleagte = deleagte;
    }

    public <NEW> ListenablePromise<NEW> transform(Promises.TransformFunction<? super T, ? extends NEW> transformer) {
        return Promises.decorate(Futures.transform(this, transformer, Promises.executor()));
    }

    public <NEW> ListenablePromise<NEW> transformAsync(Promises.AsyncTransformFunction<? super T, ? extends NEW> transformer) {
        return Promises.decorate(Futures.transformAsync(this, transformer, Promises.executor()));
    }

    public ListenablePromise<T> callback(Promises.PromiseCallback<T> callback) {
        Futures.addCallback(this, callback, Promises.executor());
        return this;
    }

    public T unchecked() {
        return Futures.getUnchecked(this);
    }

    public ListenablePromise<T> logException() {
        return callback((ignore, throwable) -> {
            if (throwable != null) {
                LOGGER.error(throwable, "promise fail:" + this);
                return;
            }
        });
    }

    @Override
    @Deprecated
    public void addListener(Runnable listener, Executor executor) {
        this.deleagte.addListener(listener, executor);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return this.deleagte.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return this.deleagte.isCancelled();
    }

    @Override
    public boolean isDone() {
        return this.deleagte.isDone();
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        return this.deleagte.get();
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return this.deleagte.get(timeout, unit);
    }
}

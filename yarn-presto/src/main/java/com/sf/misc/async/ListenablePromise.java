package com.sf.misc.async;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

public class ListenablePromise<T> implements ListenableFuture<T> {

    public static final Logger LOGGER = Logger.get(ListenablePromise.class);

    public static interface PromiseCallback<T> extends FutureCallback<T> {

        public void onComplete(T result, Throwable throwable) throws Throwable;

        default void onSuccess(T result) {
            try {
                onComplete(result, null);
            } catch (Throwable throwable) {
                onFailure(throwable);
            }
        }

        default void onFailure(Throwable t) {
            try {
                onComplete(null, t);
            } catch (Throwable throwable) {
                LOGGER.warn(t, "fail of complete promise:" + this);
            }
        }
    }

    public static interface PromiseSuccessOnlyCallback<T> extends PromiseCallback<T> {

        public void callback(T result) throws Throwable;

        public default void onComplete(T result, Throwable throwable) throws Throwable {
            if (throwable != null) {
                LOGGER.warn(throwable, "promise fail:" + this);
                return;
            }

            callback(result);
        }
    }

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

    public ListenablePromise<T> callback(PromiseCallback<T> callback) {
        Futures.addCallback(this, callback, Promises.executor());
        return this;
    }

    public ListenablePromise<T> callback(PromiseSuccessOnlyCallback<T> callback) {
        Futures.addCallback(this, callback, Promises.executor());
        return this;
    }

    public ListenablePromise<T> orFail(Supplier<ListenablePromise<T>> when_fail) {
        SettablePromise<T> settable = SettablePromise.create();
        this.callback((result, exception) -> {
            if (exception == null) {
                settable.set(result);
                return;
            }

            LOGGER.warn(exception, "promise fail:" + this);
            settable.setFuture(when_fail.get());
        });

        return settable;
    }

    public ListenablePromise<T> whenFail(Promises.UncheckedRunnable callback) {
        this.callback((ignore, exception) -> {
            if (exception != null) {
                LOGGER.warn(exception, "promise fail:" + callback);
                callback.run();
                return;
            }

            return;
        });

        return this;
    }

    public T unchecked() {
        return Futures.getUnchecked(this);
    }

    public ListenablePromise<T> logException() {
        return logException(() -> "promise fail:" + this);
    }

    public ListenablePromise<T> logException(Supplier<String> message) {
        return callback((ignore, throwable) -> {
            if (throwable != null) {
                LOGGER.error(throwable, message.get());
                return;
            }
        });
    }

    protected ListenableFuture<T> delegate() {
        return deleagte;
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

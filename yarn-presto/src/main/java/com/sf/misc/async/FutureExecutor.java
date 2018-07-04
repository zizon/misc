package com.sf.misc.async;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import java.util.concurrent.Callable;
import java.util.function.Function;

public class FutureExecutor {

    public static ListeningExecutorService executor() {
        return ExecutorServices.executor();
    }

    public static <Input, Output> ListenableFuture<Output> transform(ListenableFuture<Input> input, Functional.ExceptionalFunction<? super Input, ? extends Output> function) {
        return Futures.transform(input, function, executor());
    }

    public static <Input, Output> ListenableFuture<Output> transformAsync(ListenableFuture<Input> input, Functional.ExceptionalAsyncFunction<? super Input, ? extends Output> function) {
        return Futures.transformAsync(input, function, executor());
    }

    public static <V> void addCallback(
            final ListenableFuture<V> future,
            final Functional.ExceptionalFutureCallback<? super V> callback) {
        Futures.addCallback(future, callback, executor());
    }

    public static <V> ListenableFuture<V> catching(
            ListenableFuture<? extends V> input,
            Functional.ExceptionalFunction<Throwable, ? extends V> fallback) {
        return Futures.catching(input, Throwable.class, fallback, executor());
    }
}

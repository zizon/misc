package com.sf.misc.async;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.JdkFutureAdapters;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.airlift.log.Logger;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.checkerframework.checker.nullness.compatqual.NullableDecl;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BinaryOperator;


public class Promises {

    public static final Logger LOGGER = Logger.get(Promises.class);

    public static interface PromiseRunnable extends Runnable {
        public void apply() throws Throwable;

        default public void run() {
            try {
                this.apply();
            } catch (Throwable excetpion) {
                throw new RuntimeException("fail to apply", excetpion);
            }
        }
    }

    public static interface TransformFunction<Input, Output> extends Function<Input, Output> {
        public Output applyExceptional(@NullableDecl Input input) throws Throwable;

        default public Output apply(@NullableDecl Input input) {
            try {
                return applyExceptional(input);
            } catch (Throwable throwable) {
                throw new RuntimeException(throwable);
            }
        }
    }

    public static interface AsyncTransformFunction<Input, Output> extends AsyncFunction<Input, Output> {
    }

    public static interface PromiseCallback<T> extends FutureCallback<T> {

        public void onSuccessExceptional(@NullableDecl T result, Throwable throwable) throws Throwable;

        default void onSuccess(@NullableDecl T result) {
            try {
                onSuccessExceptional(result, null);
            } catch (Throwable throwable) {
                onFailure(throwable);
            }
        }

        default void onFailure(Throwable t) {
            try {
                onSuccessExceptional(null, t);
            } catch (Throwable throwable) {
                LOGGER.error(t, "fail of complete promise:" + this);
            }
        }
    }

    public static interface Function2<A, B, R> {
        public R apply(A a, B b) throws Throwable;
    }

    public static class PromiseCombiner<A, B, R> {

        protected final List<ListenablePromise<?>> promises;

        public PromiseCombiner(List<ListenablePromise<?>> promises) {
            this.promises = promises;
        }

        public ListenablePromise<R> call(Function2<A, B, R> function) {
            return promises.parallelStream().reduce((left, right) -> {
                return left.transformAsync((ignore) -> right);
            }).get().transform((ignore) -> {
                return function.apply((A) promises.get(0).unchecked(), (B) promises.get(1).unchecked());
            });
        }
    }

    protected static ScheduledExecutorService SCHEDULE = Executors.newScheduledThreadPool(1);
    protected static ListeningExecutorService EXECUTOR = MoreExecutors.listeningDecorator(
            Executors.newWorkStealingPool(Math.max(8, Runtime.getRuntime().availableProcessors())));

    public static <T> ListenablePromise<T> submit(Callable<T> callable) {
        return decorate(executor().submit(callable));
    }

    public static ListenablePromise<?> submit(PromiseRunnable runnable) {
        return decorate(executor().submit(runnable));
    }

    public static ListenablePromise<?> schedule(PromiseRunnable runnable, long period) {
        return decorate( //
                JdkFutureAdapters.listenInPoolThread( //
                        SCHEDULE.scheduleAtFixedRate(runnable, 0, period, TimeUnit.MILLISECONDS), //
                        executor() //
                )
        );
    }

    public static <T> ListenablePromise<T> delay(Callable<T> callable, long delay_miliseconds) {
        return Promises.decorate( //
                Futures.scheduleAsync( //
                        () -> submit(callable),  //
                        delay_miliseconds,  //
                        TimeUnit.MILLISECONDS, //
                        SCHEDULE //
                ) //
        );
    }

    public static <T> ListenablePromise<T> immediate(T value) {
        return decorate(Futures.immediateFuture(value));
    }

    public static <T> ListenablePromise<T> failure(Throwable throwable) {
        return decorate(Futures.immediateFailedFuture(throwable));
    }

    public static <T> ListenablePromise<T> decorate(ListenableFuture<T> target) {
        return new ListenablePromise<>(target);
    }

    public static <A, B, R> PromiseCombiner<A, B, R> chain(ListenablePromise<A> a, ListenablePromise<B> b) {
        return new PromiseCombiner<A, B, R>(ImmutableList.of(a, b));
    }

    public static ListeningExecutorService executor() {
        return EXECUTOR;
    }

    public static <T> ListenablePromise<T> retry(Callable<Optional<T>> invokable) {
        // retry
        return submit(new Callable<ListenablePromise<T>>() {
            int retryied = -1;
            SettablePromise<T> future = SettablePromise.create();
            RetryPolicy policy = RetryPolicies.exponentialBackoffRetry(30, 200, TimeUnit.MILLISECONDS);

            @Override
            public ListenablePromise<T> call() throws Exception {
                RetryPolicy.RetryAction action = policy.shouldRetry(null, retryied++, 0, true);
                if (action.action == RetryPolicy.RetryAction.RetryDecision.FAIL) {
                    future.setException(new RuntimeException("max retry(30) failed for:" + invokable));
                    return future;
                }

                delay(() -> invokable.call(), action.delayMillis).callback((retried, failure) -> {
                    if (failure == null && retried.isPresent()) {
                        future.set(retried.get());
                        return;
                    }

                    if (failure != null) {
                        LOGGER.warn("fail of retry:" + retryied + " invokable:" + invokable + " retry wait...");
                    }

                    if (!future.isCancelled()) {
                        this.call();
                    } else {
                        LOGGER.warn("invokable:" + invokable + " cancelled");
                    }
                });

                return future;
            }
        }).transformAsync((future) -> future);
    }

    public static <T, C extends Collection<T>> BinaryOperator<ListenablePromise<C>> reduceCollectionsOperator() {
        return (left, right) -> {
            return left.transformAsync((left_collections) -> {
                return right.transformAsync((right_collections) -> {
                    left_collections.addAll(right_collections);
                    return left;
                });
            });
        };
    }
}

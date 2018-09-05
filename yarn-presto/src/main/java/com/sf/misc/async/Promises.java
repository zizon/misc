package com.sf.misc.async;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
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

import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.stream.Collectors;


public class Promises {

    public static final Logger LOGGER = Logger.get(Promises.class);

    public static interface TransformFunction<Input, Output> extends Function<Input, Output> {
        public Output applyThrowable(Input input) throws Throwable;

        default public Output apply(Input input) {
            try {
                return applyThrowable(input);
            } catch (Throwable throwable) {
                throw new RuntimeException(throwable);
            }
        }
    }

    public static interface AsyncTransformFunction<Input, Output> extends AsyncFunction<Input, Output> {
    }

    public static interface Function2<A, B, R> {
        public R apply(A a, B b) throws Throwable;
    }

    public static interface UncheckedCallable<T> extends Callable<T> {
        public T callThrowable() throws Throwable;

        default public T call() throws Exception {
            try {
                return this.callThrowable();
            } catch (Throwable throwable) {
                throw new RuntimeException("fail to call", throwable);
            }
        }
    }

    public static interface UncheckedRunnable extends Runnable {
        public void runThrowable() throws Throwable;

        default public void run() {
            try {
                this.runThrowable();
            } catch (Throwable excetpion) {
                throw new RuntimeException("fail to apply", excetpion);
            }
        }
    }

    public static interface UncheckedConsumer<T> extends Consumer<T> {
        public void acceptThrowable(T t) throws Throwable;

        default public void accept(T t) {
            try {
                this.acceptThrowable(t);
            } catch (Throwable throwable) {
                throw new RuntimeException("fail to counsume", throwable);
            }
        }
    }

    public static class PromiseCombiner<A, B, R> {

        protected final ListenablePromise<A> left;
        protected final ListenablePromise<B> right;

        public PromiseCombiner(ListenablePromise<A> left, ListenablePromise<B> right) {
            this.left = left;
            this.right = right;
        }

        public ListenablePromise<R> call(Function2<A, B, R> function) {
            return left.transformAsync((left_result) -> {
                return right.transform((right_result) -> {
                    return function.apply(left_result, right_result);
                });
            });
        }
    }

    protected static ScheduledExecutorService SCHEDULE_EXECUTOR = Executors.newScheduledThreadPool(1);
    protected static ExecutorService BLOCKING_CALL_EXECUTOR = Executors.newCachedThreadPool();
    protected static ListeningExecutorService EXECUTOR = MoreExecutors.listeningDecorator( //
            Executors.newWorkStealingPool( //
                    Math.max(8, Runtime.getRuntime().availableProcessors()) //
            )
    );

    public static <T> ListenablePromise<T> submit(UncheckedCallable<T> callable) {
        return decorate(executor().submit(callable));
    }

    public static ListenablePromise<?> submit(UncheckedRunnable runnable) {
        return decorate(executor().submit(runnable));
    }

    public static ListenablePromise<?> schedule(UncheckedRunnable runnable, long period, boolean auto_resume) {
        CallbackResistedListenablePromise<?> resisted = new CallbackResistedListenablePromise<>( //
                decorate( //
                        JdkFutureAdapters.listenInPoolThread( //
                                SCHEDULE_EXECUTOR.scheduleAtFixedRate( //
                                        runnable,  //
                                        0,  //
                                        period,  //
                                        TimeUnit.MILLISECONDS //
                                ), //
                                BLOCKING_CALL_EXECUTOR
                        ) //
                ) //
        );

        // on failure
        resisted.logException((throwable) -> {
            // restart
            if (auto_resume) {
                Promises.delay(
                        () -> {
                            // cut from old promise
                            ListenableFuture<?> future = JdkFutureAdapters.listenInPoolThread( //
                                    SCHEDULE_EXECUTOR.scheduleAtFixedRate( //
                                            runnable,  //
                                            0,  //
                                            period,  //
                                            TimeUnit.MILLISECONDS //
                                    ), //
                                    BLOCKING_CALL_EXECUTOR
                            );
                            future.addListener(() -> {
                                try {
                                    Object result = Futures.getUnchecked(future);
                                    resisted.callbacks().parallelStream().forEach((callback) -> ((FutureCallback) callback).onSuccess(result));
                                } catch (Throwable exception) {
                                    resisted.callbacks().parallelStream().forEach((callback) -> ((FutureCallback) callback).onFailure(exception));
                                }

                                return;
                            }, Promises.executor());

                            return future;
                        }, //
                        period //
                );
            }

            return "fail when scheduling, restart...";
        });

        return resisted;
    }

    public static <T> ListenablePromise<T> delay(UncheckedCallable<T> callable, long delay_miliseconds) {
        return Promises.decorate( //
                Futures.scheduleAsync( //
                        () -> submit(callable),  //
                        delay_miliseconds,  //
                        TimeUnit.MILLISECONDS, //
                        SCHEDULE_EXECUTOR //
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

    public static <A, B, R> PromiseCombiner<A, B, R> chain(ListenablePromise<A> a, ListenablePromise<B> b, Class<R> type_inference) {
        return new PromiseCombiner<A, B, R>(a, b);
    }

    public static ListeningExecutorService executor() {
        return EXECUTOR;
    }

    public static <T> ListenablePromise<T> retry(UncheckedCallable<Optional<T>> invokable, RetryPolicy policy) {
        return retry(invokable, policy, 0);
    }

    public static <T> ListenablePromise<T> retry(UncheckedCallable<Optional<T>> invokable) {
        return retry(invokable,
                // retry with exponential backoff , up to max retries unless no exception specified
                new RetryPolicy() {
                    protected final int MAX_RETRIES = 5;
                    protected final int SLEEP_INTERVAL = 200;
                    protected final RetryPolicy delegate = RetryPolicies.exponentialBackoffRetry(MAX_RETRIES, SLEEP_INTERVAL, TimeUnit.MILLISECONDS);
                    protected final ListenablePromise<RetryAction> spin_retry = Promises.submit(
                            // add one more retrys to make sleep consisitent
                            () -> RetryPolicies.exponentialBackoffRetry(
                                    MAX_RETRIES + 1,
                                    SLEEP_INTERVAL,
                                    TimeUnit.MILLISECONDS
                            ).shouldRetry(null, MAX_RETRIES, 0, true));

                    @Override
                    public RetryAction shouldRetry(Exception e, int retries, int failovers, boolean isIdempotentOrAtMostOnce) throws Exception {
                        RetryAction action = delegate.shouldRetry(e, retries, failovers, isIdempotentOrAtMostOnce);
                        if (action == RetryAction.FAIL) {
                            if (e == null) {
                                return spin_retry.get();
                            }
                        }

                        return action;
                    }
                });
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

    protected static <T> ListenablePromise<T> retry(UncheckedCallable<Optional<T>> invokable, RetryPolicy policy, int retried) {
        return retry(invokable, policy, retried, new RuntimeException("stacktrace"));
    }

    protected static <T> ListenablePromise<T> retry(UncheckedCallable<Optional<T>> invokable, RetryPolicy policy, int retried, Throwable stacktrace) {
        SettablePromise<T> future = SettablePromise.create();

        ListenablePromise<Optional<T>> direct_future = submit(invokable).callback((result, exception) -> {
            if (exception == null && result.isPresent()) {
                future.set(result.get());
                return;
            }

            // if should retry?
            RetryPolicy.RetryAction action = null;
            if (exception instanceof Exception) {
                action = policy.shouldRetry((Exception) exception, retried, 0, true);
            } else {
                // ignore exception?
                action = policy.shouldRetry(null, retried, 0, true);
            }

            if (action == RetryPolicy.RetryAction.FAIL) {
                if (exception == null) {
                    exception = new RuntimeException("fail to do invoke:" + invokable + " retried:" + retried + " policy:" + policy, stacktrace);
                }

                future.setException(exception);
                return;
            }

            // cacneld
            if (future.isCancelled()) {
                LOGGER.warn("invokable:" + invokable + " cancelled", stacktrace);
                return;
            }

            // do retry
            future.setFuture( //
                    delay( //
                            () -> retry(invokable, policy, retried + 1, stacktrace),
                            action.delayMillis
                    ).transformAsync((throught) -> throught)
            );
            return;
        });

        return future;
    }
}

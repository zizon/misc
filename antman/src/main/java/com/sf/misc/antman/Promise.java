package com.sf.misc.antman;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Promise<T> extends CompletableFuture<T> implements ListenableFuture<T> {

    public static final Log LOGGER = LogFactory.getLog(Promise.class);

    public static interface PromiseCallable<T> extends Callable<T> {

        public abstract T exceptionalCall() throws Throwable;

        default public T call() {
            try {
                return this.exceptionalCall();
            } catch (Throwable throwable) {
                throw new RuntimeException("unexpected call exception", throwable);
            }
        }
    }

    public static interface PromiseRunnable extends Runnable {
        public abstract void exceptionalRun() throws Throwable;

        default public void run() {
            try {
                this.exceptionalRun();
            } catch (Throwable throwable) {
                throw new RuntimeException("unexpected run exception", throwable);
            }
        }
    }

    public static interface PromiseFunction<T, R> extends Function<T, R> {

        public abstract R internalApply(T t) throws Throwable;

        default public R apply(T t) {
            try {
                return this.internalApply(t);
            } catch (Throwable throwable) {
                throw new RuntimeException("unexpected function exception", throwable);
            }
        }
    }

    public static interface PromiseConsumer<T> extends Consumer<T> {
        public abstract void internalAccept(T t) throws Throwable;

        default public void accept(T t) {
            try {
                this.internalAccept(t);
            } catch (Throwable throwable) {
                throw new RuntimeException("unexpected consume exception", throwable);
            }
        }
    }

    public static interface PromiseBiConsumer<T, U> extends BiConsumer<T, U> {
        public abstract void internalAccept(T first, U second) throws Throwable;

        default public void accept(T t, U u) {
            try {
                this.internalAccept(t, u);
            } catch (Throwable throwable) {
                throw new RuntimeException("unexpected biconsume exception", throwable);
            }
        }
    }

    public static interface PromiseBiFunction<T, U, R> extends BiFunction<T, U, R> {
        public abstract R internalApply(T first, U second) throws Throwable;

        default public R apply(T t, U u) {
            try {
                return this.internalApply(t, u);
            } catch (Throwable throwable) {
                throw new RuntimeException("unexpected biconsume exception", throwable);
            }
        }
    }

    public static interface PromiseSupplier<T> extends Supplier<T> {
        public abstract T internalGet() throws Throwable;

        default public T get() {
            try {
                return this.internalGet();
            } catch (Throwable throwable) {
                throw new RuntimeException("unexpected supplier exception", throwable);
            }
        }
    }

    protected static ExecutorService BLOKING = Executors.newCachedThreadPool(
            new ThreadFactoryBuilder().setNameFormat("blocking-pool-%d").build()
    );

    protected static ForkJoinPool NONBLOCKING = new ForkJoinPool( //
            Math.max(Runtime.getRuntime().availableProcessors(), 4), //
            new ForkJoinPool.ForkJoinWorkerThreadFactory() {
                AtomicLong count = new AtomicLong(0);

                @Override
                public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
                    ForkJoinWorkerThread thread = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
                    thread.setName("nonblocking-pool-" + count.getAndIncrement());

                    return thread;
                }
            },
            null,
            true
    );
    protected static ScheduledExecutorService SCHEDULER = Executors.newScheduledThreadPool(1,
            new ThreadFactoryBuilder().setNameFormat("schedule-pool-%d").build()
    );

    protected static ExecutorService DIRECT = new AbstractExecutorService() {
        @Override
        public void execute(Runnable command) {
            command.run();
        }

        @Override
        public void shutdown() {
        }

        @Override
        public List<Runnable> shutdownNow() {
            return Collections.emptyList();
        }

        @Override
        public boolean isShutdown() {
            return false;
        }

        @Override
        public boolean isTerminated() {
            return false;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return false;
        }
    };

    public static ForkJoinPool nonblocking() {
        return NONBLOCKING;
    }

    public static ExecutorService blocking() {
        return BLOKING;
    }

    public static ScheduledExecutorService scheduler() {
        return SCHEDULER;
    }

    public static ExecutorService direct() {
        return DIRECT;
    }

    public static <T> Promise<T> wrap(Future<T> future) {
        Promise<T> promise = new Promise<T>() {
            // take over control
            public boolean cancel(boolean mayInterruptIfRunning) {
                super.cancel(mayInterruptIfRunning);
                return future.cancel(mayInterruptIfRunning);
            }
        };

        // listen to future
        blocking().execute(() -> {
            try {
                promise.complete(future.get());
            } catch (Throwable throwable) {
                promise.completeExceptionally(throwable);
            }
        });

        return promise;
    }

    public static Promise<?> submit(PromiseRunnable runnable) {
        return Promise.wrap(blocking().submit(runnable));
    }

    public static <T> Promise<T> submit(PromiseCallable<T> callable) {
        return Promise.wrap(blocking().submit(callable));
    }

    public static <T> Promise<T> promise() {
        return new Promise<>();
    }

    public static <T> Promise<T> success(T value) {
        Promise<T> promise = new Promise<>();
        promise.complete(value);
        return promise;
    }

    public static <T> Promise<T> exceptional(PromiseSupplier<Throwable> supplier) {
        Promise<T> promise = new Promise<>();
        try {
            promise.completeExceptionally(supplier.get());
        } catch (Throwable throwable) {
            promise.completeExceptionally(new RuntimeException("fail when generating exeption", throwable));
        }
        return promise;
    }

    public static Promise<Void> period(PromiseRunnable runnable, long period, PromiseConsumer<Throwable> when_exception) {
        if (runnable == null) {
            return success(null);
        }

        return Promise.wrap(scheduler() //
                .scheduleAtFixedRate(() -> {
                    try {
                        runnable.run();
                    } catch (Throwable throwable) {
                        if (when_exception != null) {
                            Promise.submit(() -> when_exception.accept(throwable)).logException();
                        } else {
                            // fallback log
                            LOGGER.error("scheudler period fail, restart:" + runnable, throwable);
                        }
                    }
                }, 0, period, TimeUnit.MILLISECONDS)
        ).transform((ignore) -> null);
    }

    public static Promise<Void> period(PromiseRunnable runnable, long period) {
        return period(runnable, period, null);
    }

    public static Promise<Void> delay(PromiseRunnable runnable, long delay, PromiseConsumer<Throwable> when_exception) {
        if (runnable == null) {
            return success(null);
        }

        return Promise.wrap(scheduler() //
                .schedule(() -> {
                    try {
                        runnable.run();
                    } catch (Throwable throwable) {
                        if (when_exception != null) {
                            Promise.submit(() -> when_exception.accept(throwable)).logException();
                        } else {
                            // fallback log
                            LOGGER.error("scheudler delay fail:" + runnable, throwable);
                        }
                    }
                }, delay, TimeUnit.MILLISECONDS)
        ).transform((ignore) -> null);
    }

    public static Promise<Void> delay(PromiseRunnable runnable, long delay) {
        return delay(runnable, delay, null);
    }

    public static Promise<Void> all(CompletableFuture<?>... cfs) {
        return wrap(CompletableFuture.allOf(cfs));
    }

    public static Promise<Void> all(Stream<? extends CompletableFuture<?>> cfs) {
        return all(cfs.parallel().filter((future) -> {
            return !future.isDone() || ((CompletableFuture) future).isCompletedExceptionally();
        }).toArray(Promise[]::new));
    }

    protected Promise() {
        super();
    }

    public <R> Promise<R> transform(PromiseFunction<T, R> function) {
        Promise<R> promise = new Promise<>();

        // success
        this.thenAcceptAsync((value) -> {
            try {
                promise.complete(function.apply(value));
            } catch (Throwable throwable) {
                promise.completeExceptionally(throwable);
            }
        }, usingExecutor());

        // exception
        this.catching(promise::completeExceptionally);
        return promise;
    }

    public <R> Promise<R> transformAsync(PromiseFunction<T, Promise<R>> function) {
        Promise<R> promise = new Promise<>();

        // success
        this.thenAcceptAsync((value) -> {
            try {
                Promise<R> transformed = function.apply(value);

                // success
                transformed.thenAcceptAsync((produced) -> {
                    promise.complete(produced);
                }, usingExecutor());

                // exeption
                transformed.catching(promise::completeExceptionally);
            } catch (Throwable throwable) {
                promise.completeExceptionally(throwable);
            }
        }, usingExecutor());

        // exception
        this.catching(promise::completeExceptionally);
        return promise;
    }

    public Promise<T> catching(PromiseConsumer<Throwable> when_exception) {
        // exception
        this.exceptionally((throwable) -> {
            try {
                when_exception.accept(throwable);
            } catch (Throwable exceptoin) {
                LOGGER.warn("fail catching promise", throwable);
            }
            return null;
        });

        return this;
    }

    public Promise<T> logException() {
        return this.catching((throwable) -> {
            LOGGER.error("fail promise", throwable);
        });
    }

    public Promise<T> fallback(PromiseSupplier<T> supplier) {
        Promise<T> promise = new Promise<>();

        // success
        this.thenAcceptAsync(promise::complete, usingExecutor());

        // exeption
        this.exceptionally((throwable) -> {
            try {
                promise.complete(supplier.get());
            } catch (Throwable exception) {
                promise.completeExceptionally(exception);
            }

            return null;
        });

        return promise;
    }

    public Optional<T> maybe() {
        try {
            return Optional.ofNullable(this.join());
        } catch (Throwable e) {
            return Optional.empty();
        }
    }

    public Promise<T> costly() {
        Promise<T> promise = new Promise<T>() {
            protected ExecutorService usingExecutor() {
                return blocking();
            }
        };

        // success
        this.thenAcceptAsync(promise::complete, usingExecutor());

        // exception
        this.exceptionally((throwable) -> {
            promise.completeExceptionally(throwable);
            return null;
        });

        return promise;
    }

    public Promise<T> sidekick(PromiseConsumer<T> callback) {
        this.thenAcceptAsync(callback, usingExecutor());
        return this;
    }

    public Promise<T> sidekick(PromiseRunnable runnable) {
        this.thenRunAsync(runnable, usingExecutor());
        return this;
    }

    public Promise<T> addListener(PromiseRunnable listener) {
        this.addListener(listener, usingExecutor());
        return this;
    }

    protected ExecutorService usingExecutor() {
        return nonblocking();
    }

    @Override
    public void addListener(Runnable listener, Executor executor) {
        this.thenRunAsync(listener, executor).exceptionally((throwable) -> {
            LOGGER.warn("invoke listener fail", throwable);
            return null;
        });

        this.exceptionally((throwable) -> {
            listener.run();
            return null;
        });
    }

}

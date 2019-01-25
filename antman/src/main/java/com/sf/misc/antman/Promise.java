package com.sf.misc.antman;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Arrays;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class Promise<T> extends CompletableFuture<T> {

    public static final Log LOGGER = LogFactory.getLog(Promise.class);

    public static interface PromiseCallable<T> extends Callable<T> {

        public abstract T exceptionalCall() throws Throwable;

        default public T call() {
            try {
                return this.exceptionalCall();
            } catch (RuntimeException e) {
                throw e;
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
            } catch (RuntimeException e) {
                throw e;
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
            } catch (RuntimeException e) {
                throw e;
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
            } catch (RuntimeException e) {
                throw e;
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
            } catch (RuntimeException e) {
                throw e;
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
            } catch (RuntimeException e) {
                throw e;
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
            } catch (RuntimeException e) {
                throw e;
            } catch (Throwable throwable) {
                throw new RuntimeException("unexpected supplier exception", throwable);
            }
        }
    }

    public static interface PromiseExecutor {
        public <T> Promise<T> submit(PromiseCallable<T> callable);

        public Executor executor();

        default public ForkJoinPool forkjoin() {
            return ForkJoinPool.commonPool();
        }
    }

    protected static ThreadFactory createFactory(String prefix) {
        return new ThreadFactory() {
            AtomicLong counter = new AtomicLong(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, prefix + counter.incrementAndGet());
            }
        };
    }

    protected static PromiseExecutor BLOKING = new PromiseExecutor() {
        protected final ExecutorService delegate = Executors.newCachedThreadPool(createFactory("blocking-pool-"));

        @Override
        public <T> Promise<T> submit(PromiseCallable<T> callable) {
            Promise<T> promise = new Promise<>();
            delegate.execute(() -> {
                try {
                    promise.complete(callable.call());
                } catch (Throwable throwable) {
                    promise.completeExceptionally(throwable);
                }
            });

            return promise;
        }

        @Override
        public Executor executor() {
            return this.delegate;
        }
    };

    protected static PromiseExecutor NONBLOCKING = new PromiseExecutor() {
        protected final ForkJoinPool delegate = new ForkJoinPool( //
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

        @Override
        public <T> Promise<T> submit(PromiseCallable<T> callable) {
            Promise<T> promise = new Promise<>();
            delegate.execute(() -> {
                try {
                    promise.complete(callable.call());
                } catch (Throwable throwable) {
                    promise.completeExceptionally(throwable);
                }
            });
            return promise;
        }

        @Override
        public Executor executor() {
            return this.delegate;
        }

        @Override
        public ForkJoinPool forkjoin() {
            return this.delegate;
        }
    };

    protected static ScheduledExecutorService SCHEDULER = Executors.newScheduledThreadPool(1, createFactory("schedule-pool-"));

    public static PromiseExecutor nonblocking() {
        return NONBLOCKING;
    }

    public static PromiseExecutor blocking() {
        return BLOKING;
    }

    public static ScheduledExecutorService scheduler() {
        return SCHEDULER;
    }

    protected static final Queue<FutureCompleteCallback> PENDING_FUTRE = new ConcurrentLinkedQueue<>();

    protected static interface FutureCompleteCallback {
        public Future<?> future();

        public void onDone();

        public void onCancle();

        default public boolean callback() {
            Future<?> future = future();
            if (future == null) {
                return true;
            }

            if (future.isDone()) {
                onDone();
                return true;
            } else if (future.isCancelled()) {
                onCancle();
                return true;
            }

            return false;
        }
    }

    static {
        period(() -> {
            // collect
            PENDING_FUTRE.removeIf(FutureCompleteCallback::callback);
        }, 100);

        period(() -> {
            LOGGER.info("pending:" + PENDING_FUTRE.size());
        }, 5000);
    }

    public static <T> Promise<T> wrap(Future<T> future) {
        if (future instanceof Promise) {
            return Promise.class.cast(future);
        } else if (future instanceof CompletableFuture) {
            Promise<T> promise = new Promise<T>() {
                public boolean cancel(boolean mayInterruptIfRunning) {
                    return future.cancel(mayInterruptIfRunning);
                }
            };

            ((CompletableFuture<T>) future).whenCompleteAsync((value, exception) -> {
                if (exception != null) {
                    promise.completeExceptionally(exception);
                    return;
                }

                promise.complete(value);
            }, nonblocking().executor());

            return promise;
        }

        Promise<T> promise = new Promise<T>() {
            public boolean cancel(boolean mayInterruptIfRunning) {
                return future.cancel(mayInterruptIfRunning);
            }
        };

        PENDING_FUTRE.offer(new FutureCompleteCallback() {
            @Override
            public Future<?> future() {
                return future;
            }

            @Override
            public void onDone() {
                try {
                    promise.complete(future.get());
                } catch (Throwable e) {
                    promise.completeExceptionally(e);
                }
            }

            @Override
            public void onCancle() {
                try {
                    promise.cancel(true);
                } catch (Throwable e) {
                    promise.completeExceptionally(e);
                }
            }
        });

        return promise;
    }

    public static <T> Promise<T> costly(PromiseCallable<T> callable) {
        return blocking().submit(callable);
    }

    public static Promise<?> costly(PromiseRunnable runnable) {
        return costly(() -> {
            runnable.run();
            return null;
        });
    }

    public static <T> Promise<T> light(PromiseCallable<T> callable) {
        return nonblocking().submit(callable);
    }

    public static Promise<Void> light(PromiseRunnable runnable) {
        return light(() -> {
            runnable.run();
            return null;
        });
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

    public static Promise<?> period(PromiseRunnable runnable, long period, PromiseConsumer<Throwable> when_exception) {
        if (runnable == null) {
            return success(null);
        }

        return Promise.wrap(scheduler() //
                .scheduleAtFixedRate(() -> {
                    try {
                        runnable.run();
                    } catch (Throwable throwable) {
                        if (when_exception != null) {
                            Promise.costly(() -> when_exception.accept(throwable)).logException();
                        } else {
                            // fallback log
                            LOGGER.error("scheudler period fail, restart:" + runnable, throwable);
                        }
                    }
                }, 0, period, TimeUnit.MILLISECONDS)
        );
    }

    public static Promise<?> period(PromiseRunnable runnable, long period) {
        return period(runnable, period, null);
    }

    public static Promise<?> delay(PromiseRunnable runnable, long delay, PromiseConsumer<Throwable> when_exception) {
        if (runnable == null) {
            return success(null);
        }

        return Promise.wrap(scheduler() //
                .schedule(() -> {
                    try {
                        runnable.run();
                    } catch (Throwable throwable) {
                        if (when_exception != null) {
                            Promise.costly(() -> when_exception.accept(throwable)).logException();
                        } else {
                            // fallback log
                            LOGGER.error("scheudler delay fail:" + runnable, throwable);
                        }
                    }
                }, delay, TimeUnit.MILLISECONDS)
        );
    }

    public static Promise<?> delay(PromiseRunnable runnable, long delay) {
        return delay(runnable, delay, null);
    }

    public static Promise<?> all(Promise<?>... promises) {
        return Arrays.stream(promises).collect(collector());
    }

    public static Collector<Promise<?>, ?, Promise<Void>> collector() {
        return Collectors.collectingAndThen(
                Collectors.reducing(
                        Promise.success(null),
                        (left, right) -> {
                            return left.transformAsync((ignore) -> right);
                        }
                ),
                (value) -> value.transform((ignore) -> null)
        );
    }

    protected Promise() {
        super();
    }

    public <R> Promise<R> transformAsync(PromiseFunction<T, Promise<R>> function) {
        Promise<T> self = this;
        Promise<R> promise = new Promise<R>() {
            public boolean cancel(boolean mayInterruptIfRunning) {
                return self.cancel(mayInterruptIfRunning);
            }
        };

        this.whenCompleteAsync((value, exception) -> {
            if (exception != null) {
                promise.completeExceptionally(exception);
                return;
            }

            try {
                function.apply(value).whenCompleteAsync((final_value, final_exception) -> {
                    if (final_exception != null) {
                        promise.completeExceptionally(final_exception);
                        return;
                    }

                    promise.complete(final_value);
                }, usingExecutor().executor());
            } catch (Throwable throwable) {
                promise.completeExceptionally(throwable);
            }

            return;
        }, usingExecutor().executor());

        return promise;
    }

    public <R> Promise<R> transform(PromiseFunction<T, R> function) {
        return this.transformAsync((value) -> Promise.success(function.apply(value)));
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
        Promise<T> self = this;
        Promise<T> promise = new Promise<T>() {
            public boolean cancel(boolean mayInterruptIfRunning) {
                return self.cancel(mayInterruptIfRunning);
            }
        };

        this.whenCompleteAsync((value, exception) -> {
            if (exception != null) {
                try {
                    promise.complete(supplier.get());
                } catch (Throwable throwable) {
                    promise.completeExceptionally(throwable);
                }
                return;
            }

            promise.complete(value);
            return;
        }, usingExecutor().executor());

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
        Promise<T> self = this;
        Promise<T> promise = new Promise<T>() {
            protected PromiseExecutor usingExecutor() {
                return blocking();
            }

            public boolean cancel(boolean mayInterruptIfRunning) {
                return self.cancel(mayInterruptIfRunning);
            }
        };

        this.whenCompleteAsync((value, exception) -> {
            if (exception != null) {
                promise.completeExceptionally(exception);
                return;
            }

            try {
                promise.complete(value);
            } catch (Throwable throwable) {
                promise.completeExceptionally(throwable);
            }
            return;
        }, usingExecutor().executor());

        return promise;
    }

    public Promise<T> sidekick(PromiseConsumer<T> callback) {
        this.thenAcceptAsync(callback, usingExecutor().executor());
        return this;
    }

    public Promise<T> sidekick(PromiseRunnable runnable) {
        this.thenRunAsync(runnable, usingExecutor().executor());
        return this;
    }

    public Promise<T> addListener(PromiseRunnable listener) {
        this.whenCompleteAsync((value, exception) -> {
            listener.run();
        }, usingExecutor().executor());
        return this;
    }

    public Promise<T> timeout(PromiseSupplier<T> provide_when_timeout, long timeout) {
        if (timeout < 0) {
            LOGGER.warn("timeout is " + timeout + " < 0 will not set timeout");
            return this;
        }

        Promise<T> self = this;
        Promise<T> timeout_value = new Promise<T>() {
            public boolean cancel(boolean mayInterruptIfRunning) {
                return self.cancel(mayInterruptIfRunning);
            }

        };

        // tiem out notifier
        Promise<?> when_tiemout = Promise.delay(() -> {
            try {
                timeout_value.complete(provide_when_timeout.get());
            } catch (Throwable throwable) {
                timeout_value.completeExceptionally(throwable);
            }

            // cancle this
            self.cancel(true);
        }, timeout);

        // inherit value if complte in time
        this.whenCompleteAsync((value, exception) -> {
            if (exception != null) {
                timeout_value.completeExceptionally(exception);
                return;
            }

            try {
                timeout_value.complete(value);
            } catch (Throwable e) {
                timeout_value.completeExceptionally(e);
            }

            // cancle timeout when finished
            when_tiemout.cancel(true);
        }, usingExecutor().executor());

        return timeout_value;
    }

    protected PromiseExecutor usingExecutor() {
        return nonblocking();
    }
}

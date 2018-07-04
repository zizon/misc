package com.sf.misc.async;

import com.google.common.base.Function;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import org.checkerframework.checker.nullness.compatqual.NullableDecl;

import java.util.function.Supplier;

public interface Functional {

    @FunctionalInterface
    public static interface ExceptionalFunction<Input, Output> extends Function<Input, Output> {
        public Output applyExceptional(@NullableDecl Input input) throws Throwable;

        default public Output apply(@NullableDecl Input input) {
            try {
                return applyExceptional(input);
            } catch (Throwable throwable) {
                throw new RuntimeException(throwable);
            }
        }
    }

    @FunctionalInterface
    public static interface ExceptionalAsyncFunction<Input, Output> extends AsyncFunction<Input, Output> {
        public ListenableFuture<Output> applyExceptional(@NullableDecl Input input) throws Throwable;

        default public ListenableFuture<Output> apply(@NullableDecl Input input) {
            try {
                return applyExceptional(input);
            } catch (Throwable throwable) {
                throw new RuntimeException(throwable);
            }
        }
    }

    @FunctionalInterface
    public static interface ExceptionalFutureCallback<V> extends FutureCallback<V> {
        public void callback(@NullableDecl V result, Throwable throwable) throws Throwable;

        default public void onSuccess(@NullableDecl V result) {
            try {
                callback(result, null);
            } catch (Throwable throwable) {
                this.onFailure(throwable);
            }
        }

        default public void onFailure(Throwable t) {
            try {
                callback(null, t);
            } catch (Throwable throwable) {
                throw new RuntimeException(throwable);
            }
        }
    }

    @FunctionalInterface
    public static interface ExceptionalFunction2<V, A, B> {
        public <V, A, B> V applyExceptional(@NullableDecl A a, @NullableDecl B b) throws Throwable;

        default public V apply(@NullableDecl A a, @NullableDecl B b) {
            try {
                return applyExceptional(a, b);
            } catch (Throwable throwable) {
                throw new RuntimeException(throwable);
            }
        }
    }

    @FunctionalInterface
    public static interface ExceptionalSupplier<T> extends Supplier<T> {
        T internalGet() throws Throwable;

        default T get() {
            try {
                return internalGet();
            } catch (Throwable throwable) {
                throw new RuntimeException(throwable);
            }
        }
    }
}

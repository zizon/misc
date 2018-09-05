package com.sf.misc.async;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Set;

public class CallbackResistedListenablePromise<T> extends ListenablePromise<T> {

    protected final Set<FutureCallback<T>> callbacks;

    public CallbackResistedListenablePromise(ListenablePromise<T> delegated, ImmutableSet<FutureCallback<T>> callbacks) {
        super(delegated.deleagte);
        this.callbacks = Sets.newConcurrentHashSet(callbacks);
    }

    public CallbackResistedListenablePromise(ListenablePromise<T> delegated) {
        this(delegated, ImmutableSet.of());
    }

    public ListenablePromise<T> callback(PromiseCallback<T> callback) {
        this.callbacks.add(callback);
        return super.callback(callback);
    }

    public ListenablePromise<T> callback(PromiseSuccessOnlyCallback<T> callback) {
        this.callbacks.add(callback);
        return super.callback(callback);
    }

    public ImmutableSet<FutureCallback<T>> callbacks() {
        return ImmutableSet.copyOf(callbacks);
    }

    public CallbackResistedListenablePromise<T> addCallbacks(Set<? extends FutureCallback<T>> callbacks) {
        Sets.difference(callbacks, this.callbacks)
                .parallelStream()
                .forEach((callback) -> {
                    Futures.addCallback(this, callback, Promises.executor());
                });
        this.callbacks.addAll(callbacks);
        return this;
    }
}

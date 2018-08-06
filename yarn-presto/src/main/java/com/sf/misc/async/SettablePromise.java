package com.sf.misc.async;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

public class SettablePromise<T> extends ListenablePromise<T> {

    protected final SettableFuture<T> settable;

    public static <T> SettablePromise<T> create() {
        return new SettablePromise<T>();
    }

    protected SettablePromise() {
        super(SettableFuture.create());
        this.settable = (SettableFuture<T>) delegate();
    }

    public boolean set(T value) {
        return settable.set(value);
    }

    public boolean setException(Throwable throwable) {
        return settable.setException(throwable);
    }

    public boolean setFuture(ListenableFuture<? extends T> future) {
        return settable.setFuture(future);
    }
}

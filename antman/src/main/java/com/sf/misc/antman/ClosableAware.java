package com.sf.misc.antman;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Optional;

public class ClosableAware<T extends AutoCloseable> implements AutoCloseable {

    public static final Log LOGGER = LogFactory.getLog(ClosableAware.class);

    public static <T extends AutoCloseable> ClosableAware<T> wrap(Promise.PromiseSupplier<T> supplier) {
        return new ClosableAware<>(supplier.get());
    }

    protected final T reference;

    protected ClosableAware(T reference) {
        this.reference = reference;
    }

    public <R> Promise<R> transform(Promise.PromiseFunction<T, R> transform) {
        return Promise.submit(() -> {
            try (AutoCloseable closeable = this) {
                return transform.apply(this.reference);
            }
        });
    }

    public Promise<T> execute(Promise.PromiseConsumer<T> consumer) {
        return Promise.submit(() -> {
            try (AutoCloseable closeable = this) {
                consumer.accept(this.reference);
                return this.reference;
            }
        });
    }

    @Override
    public void close() throws Exception {
        if (this.reference != null) {
            this.reference.close();
        }
    }
}

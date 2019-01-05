package com.sf.misc.antman;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
        try (AutoCloseable closeable = this) {
            return Promise.success(transform.apply(this.reference));
        } catch (Throwable e) {
            return Promise.exceptional(() -> e);
        }
    }

    public Promise<Void> execute(Promise.PromiseConsumer<T> consumer) {
        return this.transform((value) -> {
            consumer.accept(value);
            return null;
        });
    }

    public Promise<Void> execute(Promise.PromiseRunnable runnable) {
        return this.execute((value) -> {
            runnable.run();
        });
    }

    @Override
    public void close() throws Exception {
        if (this.reference != null) {
            this.reference.close();
        }
    }
}

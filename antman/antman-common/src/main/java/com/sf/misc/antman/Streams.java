package com.sf.misc.antman;

import java.util.Iterator;
import java.util.Optional;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Streams {

    public <T> Stream<T> generate(Promise.PromiseSupplier<Optional<T>> supplier) {
        return StreamSupport.stream(new Iterable<T>() {
            @Override
            public Iterator<T> iterator() {
                return new Iterator<T>() {
                    T next;

                    @Override
                    public boolean hasNext() {
                        next = supplier.get().orElse(null);
                        return next != null;
                    }

                    @Override
                    public T next() {
                        return next;
                    }
                };
            }
        }.spliterator(), true);
    }
}

package com.sf.misc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class ReferenceBaseCache<VALUE_TYPE> {

    private static final Log LOGGER = LogFactory.getLog(ReferenceBaseCache.class);

    public static final ScheduledExecutorService POOL = Executors.newScheduledThreadPool(1);

    protected static final Queue<WeakReference<ReferenceBaseCache<?>>> MANAGED = new ConcurrentLinkedQueue<>();

    static {
        pool().scheduleAtFixedRate(() -> {
            LOGGER.info("trigger  reference eviction...");
            long now = System.currentTimeMillis();
            Iterator<WeakReference<ReferenceBaseCache<?>>> cache_iterator = MANAGED.iterator();
            while (cache_iterator.hasNext()) {
                WeakReference<ReferenceBaseCache<?>> reference = cache_iterator.next();

                // remove garbage collected
                ReferenceBaseCache<?> instance = reference.get();
                if (instance == null) {
                    cache_iterator.remove();
                    continue;
                }

                Iterator<?> cache_entry_iterator = instance.holder.entrySet().iterator();
                while (cache_entry_iterator.hasNext()) {
                    @SuppressWarnings("unchecked")
                    Map.Entry<String, Map.Entry<WeakReference, Long>> entry = (Map.Entry<String, Map.Entry<WeakReference, Long>>) cache_entry_iterator.next();

                    Map.Entry<WeakReference, Long> value = entry.getValue();
                    if (value.getKey().get() == null) {
                        // remove garbage collected
                        cache_entry_iterator.remove();
                        LOGGER.info("remove garbage collected");
                    } else if (now - value.getValue() >= 1000 * 60 * 60) {
                        // remove last access one hour before
                        cache_entry_iterator.remove();
                        LOGGER.info("remove expired");
                    }
                }
            }
        }, 0, 10, TimeUnit.SECONDS);
    }

    protected final String name;
    protected ConcurrentMap<String, Map.Entry<Reference<VALUE_TYPE>, Long>> holder;

    public ReferenceBaseCache(String name) {
        this.holder = new ConcurrentHashMap<>();
        this.name = name;

        // register this
        MANAGED.offer(new WeakReference<>(this));
    }

    public static ScheduledExecutorService pool() {
        return POOL;
    }

    public VALUE_TYPE fetch(final String key, final Function<String, VALUE_TYPE> when_missing) {
        if (key == null) {
            return null;
        }

        // access time
        long now = System.currentTimeMillis();

        // once initializer
        AtomicReference<VALUE_TYPE> missing_value = new AtomicReference<>(null);

        // try fetch from cache
        Map.Entry<Reference<VALUE_TYPE>, Long> entry = holder.get(key);
        if (entry == null) {
            LOGGER.info("cache:" + this.name + " miss key:" + key);
            try {
                entry = POOL.submit(() -> {
                    Map.Entry<Reference<VALUE_TYPE>, Long> presented = this.holder.get(key);
                    if (presented != null) {
                        return presented;
                    }

                    // only initialize once at this call stack
                    VALUE_TYPE value = missing_value.get();
                    if (value == null) {
                        value = when_missing.apply(key);
                        missing_value.set(value);
                    }
                    return newEntry(value, now);
                }).get();
            } catch (InterruptedException | ExecutionException e) {
                LOGGER.warn("fail to async initialize value for key:" + key);

                VALUE_TYPE value = missing_value.get();
                if (value == null) {
                    value = when_missing.apply(key);
                    missing_value.set(value);
                }
                entry = newEntry(value, now);
            }

            // setup cache
            holder.put(key, entry);
        }

        // update access time
        entry.setValue(now);

        // try weak reference
        VALUE_TYPE hit = entry.getKey().get();
        if (hit != null) {
            return hit;
        }

        // garbage collected
        hit = missing_value.get();
        if (hit == null) {
            // create new
            hit = when_missing.apply(key);
        }

        return hit;
    }

    protected Reference<VALUE_TYPE> newRerence(VALUE_TYPE value) {
        return new SoftReference<>(value);
    }

    protected Map.Entry<Reference<VALUE_TYPE>, Long> newEntry(final VALUE_TYPE value, final Long access) {
        return new Map.Entry<Reference<VALUE_TYPE>, Long>() {
            private Reference<VALUE_TYPE> holder = newRerence(value);
            private Long access_time = access;

            @Override
            public Reference<VALUE_TYPE> getKey() {
                return holder;
            }

            @Override
            public Long getValue() {
                return this.access_time;
            }

            @Override
            public Long setValue(Long value) {
                Long old = this.access_time;
                this.access_time = value;
                return old;
            }
        };
    }
}

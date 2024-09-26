package com.clickhouse.client.api.internal;

import java.util.Deque;
import java.util.Iterator;
import java.util.function.Supplier;

public abstract class CachingObjectsSupplier<T> implements Supplier<T> {

    private Iterator<T> iterator;
    private Deque<T> cache;;

    /**
     * Constructs caching object supplier that uses Deque as cache. Deque may be a thread safe implementation.
     * It will be filled with {@code preallocate} number of objects.
     *
     * @param cache
     * @param preallocate
     */
    public CachingObjectsSupplier(Deque<T> cache, int preallocate) {
        this.cache = cache;
    }

    @Override
    public T get() {
        T obj;
        if (iterator.hasNext()) {
            obj = iterator.next();
        } else {
            obj = create();
            cache.addFirst(obj);
        }

        return obj;
    }

    /**
     * Resets internal iterator to begin with the first object in the cache.
     */
    public void reset() {
        iterator = cache.iterator();
    }

    public abstract T create();
}

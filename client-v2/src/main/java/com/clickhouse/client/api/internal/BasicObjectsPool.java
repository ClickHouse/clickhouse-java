package com.clickhouse.client.api.internal;

import java.util.Deque;


/**
 * Objects pool.
 * Can be used to reuse object to reduce GC pressure.
 * Thread-safety is not guaranteed. Depends on deque implementation that is
 * passed to the constructor.
 * @param <T> - type of stored objects
 */
public abstract class BasicObjectsPool<T> {

    private Deque<T> objects;

    /**
     * Creates an empty pool.
     * @param objects object queue
     */
    BasicObjectsPool(Deque<T> objects) {
        this(objects, 0);
    }

    /**
     * Creates a pool and pre-populates it with a number of objects equal to the size parameter.
     * @param objects object queue
     * @param size initial size of the object queue
     */
    public BasicObjectsPool(Deque<T> objects, int size) {
        this.objects = objects;
        for (int i = 0; i < size; i++) {
            this.objects.add(create());
        }
    }

    public T lease() {
        T obj = objects.poll();
        return obj != null ? obj : create();
    }

    public void release(T obj) {
        objects.addFirst(obj);
    }

    protected abstract T create();
}

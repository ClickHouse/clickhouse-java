package com.clickhouse.demo_service;

import java.util.Deque;

/**
 * Very basic object pool implementation.
 * It uses a deque to store objects. Leased objects are removed from the
 * deque and released objects are added back to the deque. When there is no objects in the deque,
 * a new object is created (assuming it will be returned on release).
 *
 * @param <T>
 */
public abstract class BasicObjectsPool<T> {

    private Deque<T> objects;

    BasicObjectsPool(Deque<T> objects) {
        this(objects, 0);
    }

    BasicObjectsPool(Deque<T> objects, int size) {
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

    abstract T create();
}

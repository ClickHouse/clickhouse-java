package com.clickhouse.client.api.internal;

import java.util.Deque;

public abstract class BasicObjectsPool<T> {


    private Deque<T> objects;

    BasicObjectsPool(Deque<T> objects) {
        this(objects, 0);
    }

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

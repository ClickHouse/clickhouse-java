package com.clickhouse.demo_service;

import java.util.Deque;
import java.util.Iterator;
import java.util.function.Supplier;

/**
 * This is a prepared collection of objects. It is aimed to solve the problem of
 * creating and releasing objects in a loop. After all objects are used, the collection
 * should be reset.
 *
 *
 * @param <T>
 */
public abstract class ObjectsPreparedCollection<T> implements Supplier<T> {

    private Deque<T> objects;

    private Iterator<T> iterator;

    ObjectsPreparedCollection(Deque<T> objects) {
        this(objects, 0);
    }

    ObjectsPreparedCollection(Deque<T> objects, int size) {
        this.objects = objects;
        for (int i = 0; i < size; i++) {
            this.objects.add(create());
        }
        this.reset();
    }

    @Override
    public T get() {
        T obj;
        if (iterator.hasNext()) {
            obj = iterator.next();
        } else {
            obj = create();
            objects.addFirst(obj);
        }

        return obj;
    }

    public void reset() {
        this.iterator = objects.iterator();
    }
    abstract T create();
}

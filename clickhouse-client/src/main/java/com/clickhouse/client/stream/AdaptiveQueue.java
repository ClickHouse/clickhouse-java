package com.clickhouse.client.stream;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public final class AdaptiveQueue<E> {
    private final CapacityPolicy policy;
    private final LinkedList<E> queue;

    public AdaptiveQueue(CapacityPolicy policy, E... array) {
        this.policy = policy;
        this.queue = array == null || array.length == 0 ? new LinkedList<>() : new LinkedList<>(Arrays.asList(array));
    }

    public AdaptiveQueue(CapacityPolicy policy, List<E> list) {
        this.policy = policy;
        this.queue = new LinkedList<>(list);
    }

    public synchronized void add(E e) {
        queue.add(e);
        if (policy != null) {
            policy.ensureCapacity(0);
        }
    }

    public synchronized void clear() {
        queue.clear();
        if (policy != null) {
            policy.ensureCapacity(0);
        }
    }

    public synchronized boolean offer(E e) {
        if (policy == null || policy.ensureCapacity(queue.size())) {
            queue.addLast(e);
            return true;
        }
        return false;
    }

    public synchronized E poll() {
        if (!queue.isEmpty()) {
            return queue.removeFirst();
        }
        return null;
    }

    public synchronized int size() {
        return queue.size();
    }
}

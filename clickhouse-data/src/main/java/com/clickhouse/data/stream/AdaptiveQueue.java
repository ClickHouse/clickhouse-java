package com.clickhouse.data.stream;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

@Deprecated
public interface AdaptiveQueue<E> {
    // too slow
    static final class JctoolsSpscQueue<E> implements AdaptiveQueue<E> {
        private final Queue<E> queue;

        JctoolsSpscQueue(E... array) {
            if (array == null || array.length == 0) {
                queue = new org.jctools.queues.SpscLinkedQueue<>();
            } else {
                queue = new org.jctools.queues.SpscArrayQueue<>(array.length);
                for (E e : array) {
                    queue.offer(e);
                }
            }
        }

        @Override
        public void add(E e) {
            queue.offer(e);
        }

        public void clear() {
            queue.clear();
        }

        public boolean offer(E e) {
            return queue.offer(e);
        }

        public E poll() {
            return queue.poll();
        }

        public int size() {
            return queue.size();
        }
    }

    static final class DefaultQueue<E> implements AdaptiveQueue<E> {
        private final CapacityPolicy policy;
        private final LinkedList<E> queue;

        DefaultQueue(CapacityPolicy policy, E... array) {
            this.policy = policy;
            this.queue = array == null || array.length == 0 ? new LinkedList<>()
                    : new LinkedList<>(Arrays.asList(array));
        }

        DefaultQueue(CapacityPolicy policy, List<E> list) {
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

    static <E> AdaptiveQueue<E> create(CapacityPolicy policy, E... array) {
        // AdaptiveQueue<E> queue = null;

        // try {
        // queue = new JctoolsSpscQueue<>(array);
        // } catch (Throwable e) {
        // e.printStackTrace();
        // }
        // return queue != null ? queue : new DefaultQueue<>(policy, array);
        return new DefaultQueue<>(policy, array);
    }

    static <E> AdaptiveQueue<E> create(CapacityPolicy policy, List<E> list) {
        // AdaptiveQueue<E> queue = null;

        // try {
        // queue = new JctoolsSpscQueue<>();
        // for (E e : list) {
        // queue.offer(e);
        // }
        // } catch (Throwable e) {
        // e.printStackTrace();
        // }
        // return queue != null ? queue : new DefaultQueue<>(policy, list);
        return new DefaultQueue<>(policy, list);
    }

    public void add(E e);

    public void clear();

    public boolean offer(E e);

    public E poll();

    public int size();
}

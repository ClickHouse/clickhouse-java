package com.clickhouse.data.stream;

@Deprecated
@FunctionalInterface
public interface CapacityPolicy {
    static class FixedCapacity implements CapacityPolicy {
        private final int capacity;

        protected FixedCapacity(int capacity) {
            this.capacity = capacity < 1 ? 0 : capacity;
        }

        @Override
        public boolean ensureCapacity(int current) {
            return capacity < 1 || current < capacity;
        }
    }

    static class LinearDynamicCapacity implements CapacityPolicy {
        private volatile int capacity;
        private volatile int count;

        private final int maxSize;
        private final int variation;

        protected LinearDynamicCapacity(int initialSize, int maxSize, int variation) {
            this.capacity = initialSize < 1 ? 1 : initialSize;
            this.count = 0;

            this.maxSize = maxSize < 1 ? Integer.MAX_VALUE : Math.max(maxSize, initialSize);
            this.variation = variation < 1 ? 100 : variation;
        }

        @Override
        public boolean ensureCapacity(int current) {
            if (current < capacity) {
                count = 0;
                return true;
            } else if (capacity < maxSize && ++count >= variation) { // NOSONAR
                count = 0;
                capacity++; // NOSONAR
                return true;
            }
            return false;
        }
    }

    static CapacityPolicy fixedCapacity(int capacity) {
        return new FixedCapacity(capacity);
    }

    static CapacityPolicy linearDynamicCapacity(int initialSize, int maxSize, int variation) {
        return new LinearDynamicCapacity(initialSize, maxSize, variation);
    }

    boolean ensureCapacity(int current);
}

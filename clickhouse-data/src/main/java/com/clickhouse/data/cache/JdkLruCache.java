package com.clickhouse.data.cache;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import com.clickhouse.data.ClickHouseCache;

/**
 * A simple thread-safe LRU cache based on LinkedHashMap. It's not as effient as
 * the one in Caffeine/Guava, but it requires no extra dependency.
 */
@Deprecated
public class JdkLruCache<K, V> implements ClickHouseCache<K, V> {
    static class LruCacheMap<K, V> extends LinkedHashMap<K, V> {
        private final int capacity;

        protected LruCacheMap(int capacity) {
            super(capacity, 0.75f, true);

            this.capacity = capacity;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
            return size() > capacity;
        }
    }

    /**
     * Creates a cache with given capacity and load function.
     *
     * @param <K>      type of the key
     * @param <V>      type of the value
     * @param capacity capacity
     * @param loadFunc load function
     * @return cache
     */
    public static <K, V> ClickHouseCache<K, V> create(int capacity, Function<K, V> loadFunc) {
        return new JdkLruCache<>(new LruCacheMap<>(capacity), loadFunc);
    }

    private final Map<K, V> cache;
    private final Function<K, V> loadFunc;

    protected JdkLruCache(Map<K, V> cache, Function<K, V> loadFunc) {
        if (cache == null || loadFunc == null) {
            throw new IllegalArgumentException("Non-null cache and load function are required");
        }
        this.cache = Collections.synchronizedMap(cache);
        this.loadFunc = loadFunc;
    }

    @Override
    public V get(K key) {
        return cache.computeIfAbsent(key, loadFunc);
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        return Objects.requireNonNull(clazz, "Non-null class is required").cast(cache);
    }
}

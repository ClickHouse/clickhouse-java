package com.clickhouse.data;

import java.util.function.Function;

import com.clickhouse.data.cache.CaffeineCache;
import com.clickhouse.data.cache.JdkLruCache;

/**
 * Wrapper interface depicts essential methods required by a client-side cache.
 */
@Deprecated
public interface ClickHouseCache<K, V> {
    /**
     * Default cache size.
     */
    static final int DEFAULT_CACHE_SIZE = 50;

    /**
     * Creates a cache with specific capacity and load function.
     *
     * @param <K>           type of key
     * @param <V>           type of value
     * @param capacity      capacity of the cache, zero or negative number will be
     *                      treated as {@link #DEFAULT_CACHE_SIZE}
     * @param expireSeconds seconds to expire after access
     * @param loadFunc      non-null load function
     * @return cache
     */
    static <K, V> ClickHouseCache<K, V> create(int capacity, long expireSeconds, Function<K, V> loadFunc) {
        ClickHouseCache<K, V> cache;

        try {
            cache = CaffeineCache.create(capacity, expireSeconds, loadFunc);
        } catch (Throwable e) {
            // ignore
            cache = JdkLruCache.create(capacity, loadFunc);
        }
        return cache;
    }

    /**
     * Gets value from cache if it exists.
     *
     * @param key key, in genernal should NOT be null
     * @return non-null value in general
     */
    V get(K key);

    /**
     * Gets inner cache object to gain more access.
     *
     * @param <T>   type of the cache
     * @param clazz non-null class of the cache
     * @return inner cache object
     * @throws NullPointerException when {@code clazz} is null
     */
    <T> T unwrap(Class<T> clazz);
}

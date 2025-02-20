package com.clickhouse.data.cache;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.clickhouse.data.ClickHouseCache;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * Cache based on Caffeine implementation. Please be aware that it's not really
 * a LRU cache.
 */
@Deprecated
public class CaffeineCache<K, V> implements ClickHouseCache<K, V> {
    private final Cache<K, V> cache;
    private final Function<K, V> loadFunc;

    /**
     * Creates a cache with given capacity, seconds to expire(after access), and
     * load function.
     *
     * @param <K>           type of the key
     * @param <V>           type of the value
     * @param capacity      capacity of the cache
     * @param expireSeconds seconds to expire after access
     * @param loadFunc      load function
     * @return cache
     */
    public static <K, V> ClickHouseCache<K, V> create(int capacity, long expireSeconds, Function<K, V> loadFunc) {
        return new CaffeineCache<>(capacity, expireSeconds, loadFunc);
    }

    protected CaffeineCache(int capacity, long expireSeconds, Function<K, V> loadFunc) {
        this.cache = Caffeine.newBuilder().maximumSize(capacity).expireAfterAccess(expireSeconds, TimeUnit.SECONDS)
                .build();
        this.loadFunc = Objects.requireNonNull(loadFunc, "Non-null load function is required");
    }

    @Override
    public V get(K key) {
        return cache.get(key, loadFunc);
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        return Objects.requireNonNull(clazz, "Non-null class is required").cast(cache);
    }
}

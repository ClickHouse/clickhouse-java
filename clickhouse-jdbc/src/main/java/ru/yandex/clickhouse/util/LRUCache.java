package ru.yandex.clickhouse.util;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * LRU Cache.
 *
 * @deprecated As of release 0.3.2, replaced by
 *             {@link com.clickhouse.client.ClickHouseCache} and it will be
 *             removed in 0.4.0
 */
@Deprecated
public class LRUCache<K, V> extends LinkedHashMap<K, V> {
    /**
     * Generated serial version UID.
     */
    private static final long serialVersionUID = -4987484334796469814L;

    private final int capacity;
    private final Function<K, V> loader;

    public static <K, V> Map<K, V> create(int cacheSize, Function<K, V> loader) {
        return Collections.synchronizedMap(new LRUCache<>(cacheSize, loader));
    }

    protected LRUCache(int cacheSize, Function<K, V> loader) {
        super(cacheSize, 0.75f, true);

        this.capacity = cacheSize;
        this.loader = Objects.requireNonNull(loader);
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return size() > capacity;
    }

    @Override
    public V get(Object key) {
        @SuppressWarnings("unchecked")
        K k = (K) Objects.requireNonNull(key);
        V value = super.get(k);
        if (value == null) {
            value = loader.apply(k);
            this.put(k, value);
        }

        return value;
    }
}

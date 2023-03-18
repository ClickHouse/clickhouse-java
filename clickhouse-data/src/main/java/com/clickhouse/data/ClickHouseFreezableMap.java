package com.clickhouse.data;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Freezable map. Once the map is freezed, all writes will be discarded without
 * any error.
 */
public class ClickHouseFreezableMap<K, V> implements Map<K, V> {
    public static <K, V> ClickHouseFreezableMap<K, V> of(Map<K, V> map) {
        return new ClickHouseFreezableMap<>(ClickHouseChecker.nonNull(map, "Map"));
    }

    private final AtomicBoolean freezed;
    private final Map<K, V> map;

    protected ClickHouseFreezableMap(Map<K, V> map) {
        this.freezed = new AtomicBoolean(false);
        this.map = map;
    }

    @Override
    public void clear() {
        if (!freezed.get()) {
            map.clear();
        }
    }

    @Override
    public boolean containsKey(Object key) {
        return map.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return map.containsValue(value);
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return map.entrySet();
    }

    @Override
    public V get(Object key) {
        return map.get(key);
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public Set<K> keySet() {
        return map.keySet();
    }

    @Override
    public V put(K key, V value) {
        return !freezed.get() ? map.put(key, value) : value;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        if (!freezed.get()) {
            map.putAll(m);
        }
    }

    @Override
    public V remove(Object key) {
        return !freezed.get() ? map.remove(key) : null;
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public Collection<V> values() {
        return map.values();
    }

    /**
     * Freezes the map to discard write operations.
     *
     * @return this map
     */
    public ClickHouseFreezableMap<K, V> freeze() {
        freezed.compareAndSet(false, true);
        return this;
    }

    /**
     * Unfreezes the map to accept write operations.
     *
     * @return this map
     */
    public ClickHouseFreezableMap<K, V> unfreeze() {
        freezed.compareAndSet(true, false);
        return this;
    }

    /**
     * Checks whether the map is freezed or not.
     *
     * @return true if the map is freezed; false otherwise
     */
    public boolean isFreezed() {
        return freezed.get();
    }
}
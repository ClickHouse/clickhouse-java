package com.clickhouse.data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Freezable map. Once the map is freezed, all writes will be discarded without
 * any error.
 */
@Deprecated
public class ClickHouseFreezableMap<K, V> implements Map<K, V> {
    /**
     * Creates a freezable map with initial data and optional whitelisted keys.
     *
     * @param <K>             type of the key
     * @param <V>             type of the value
     * @param map             non-null map with initial data
     * @param whiteListedKeys optional whitelisted keys
     * @return non-null freezable map
     */
    public static <K, V> ClickHouseFreezableMap<K, V> of(Map<K, V> map, K... whiteListedKeys) {
        return new ClickHouseFreezableMap<>(ClickHouseChecker.nonNull(map, "Map"), whiteListedKeys);
    }

    private final AtomicBoolean freezed;
    private final Map<K, V> map;
    private final List<K> whitelist;

    protected ClickHouseFreezableMap(Map<K, V> map, K... keys) {
        this.freezed = new AtomicBoolean(false);
        this.map = map;
        if (keys == null || keys.length == 0) {
            this.whitelist = Collections.emptyList();
        } else {
            List<K> list = new ArrayList<>(keys.length);
            for (K k : keys) {
                if (k != null && !list.contains(k)) {
                    list.add(k);
                }
            }
            this.whitelist = Collections.unmodifiableList(list);
        }
    }

    /**
     * Checks whether the given key can be used in mutation(e.g. {@code put()},
     * {@code remove()}), regardless the map is freezed or not.
     *
     * @param key non-null key
     * @return true if the given key can be used in mutation; false otherwise
     */
    protected boolean isMutable(Object key) {
        return !freezed.get() || whitelist.contains(key);
    }

    @Override
    public void clear() {
        if (freezed.get()) {
            for (K k : whitelist) {
                map.remove(k);
            }
        } else {
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
        return isMutable(key) ? map.put(key, value) : value;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        if (freezed.get()) {
            for (K k : whitelist) {
                V v = m.get(k);
                if (v != null) {
                    map.put(k, v);
                }
            }
        } else {
            map.putAll(m);
        }
    }

    @Override
    public V remove(Object key) {
        return isMutable(key) ? map.remove(key) : null;
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

    /**
     * Checks whether the given key is whitelisted, meaning corresponding entry can
     * be changed even the map is freezed.
     *
     * @param key non-null key
     * @return true if the key is whitelisted; false otherwise
     */
    public boolean isWhiteListed(K key) {
        return key != null && whitelist.contains(key);
    }
}
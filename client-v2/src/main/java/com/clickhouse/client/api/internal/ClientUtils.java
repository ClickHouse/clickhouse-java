package com.clickhouse.client.api.internal;

import org.slf4j.Logger;

import java.io.Closeable;
import java.util.Map;
import java.util.Objects;

/**
 * Class containing utility methods used across the client.
 */
public final class ClientUtils {

    private ClientUtils() {}

    public static boolean isNotBlank(String str) {
        return !isBlank(str);
    }

    public static boolean isBlank(String str) {
        return str == null || str.trim().isEmpty();
    }

    public static void quietClose(Closeable closeable, Logger log) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception e) {
                log.warn("Failed to close object " + closeable, e);
            }
        }
    }

    /**
     * Puts the entry into the map if the map does not have the key. Keys are compared ignoring case, so the map is not
     * changed if it has the key in a different case. A key that the map has is not replaced, also if its value is
     * {@code null}.
     *
     * @param map - map to update
     * @param key - key of the entry, not {@code null}
     * @param value - value of the entry
     * @param <V> - type of the values
     */
    public static <V> void putIfAbsentIgnoreCase(Map<String, V> map, String key, V value) {
        Objects.requireNonNull(key, "key");
        for (String mapKey : map.keySet()) {
            if (key.equalsIgnoreCase(mapKey)) {
                return;
            }
        }
        map.put(key, value);
    }
}

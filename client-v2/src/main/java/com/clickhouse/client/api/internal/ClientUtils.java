package com.clickhouse.client.api.internal;

import org.slf4j.Logger;

import java.io.Closeable;
import java.util.Map;
import java.util.Objects;

/**
 * Class containing utility methods used across the client.
 */
public final class ClientUtils {

    private static final int DEFAULT_CAUSE_SEARCH_DEPTH = 3;

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

    /**
     * Finds the nearest exception of the given type in the cause chain. Checks {@code throwable} first and then
     * follows up to 3 causes.
     *
     * @param throwable - exception to start from, can be {@code null}
     * @param type - type of the exception to find, not {@code null}
     * @param <T> - type of the exception to find
     * @return {@code throwable} or its cause that is an instance of {@code type}, or {@code null} if not found
     */
    public static <T extends Throwable> T findCause(Throwable throwable, Class<T> type) {
        return findCause(throwable, type, DEFAULT_CAUSE_SEARCH_DEPTH);
    }

    /**
     * Finds the nearest exception of the given type in the cause chain. Checks {@code throwable} first and then
     * follows up to {@code maxDepth} causes.
     *
     * @param throwable - exception to start from, can be {@code null}
     * @param type - type of the exception to find, not {@code null}
     * @param maxDepth - maximum number of causes to follow; {@code 0} checks only {@code throwable}
     * @param <T> - type of the exception to find
     * @return {@code throwable} or its cause that is an instance of {@code type}, or {@code null} if not found
     * @throws IllegalArgumentException if {@code maxDepth} is negative
     */
    public static <T extends Throwable> T findCause(Throwable throwable, Class<T> type, int maxDepth) {
        Objects.requireNonNull(type, "type");
        if (maxDepth < 0) {
            throw new IllegalArgumentException("maxDepth must not be negative: " + maxDepth);
        }
        Throwable current = throwable;
        for (int depth = 0; current != null; depth++) {
            if (type.isInstance(current)) {
                return type.cast(current);
            }
            if (depth == maxDepth) {
                break;
            }
            current = current.getCause();
        }
        return null;
    }
}

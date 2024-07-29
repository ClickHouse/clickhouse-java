package com.clickhouse.client.api.internal;


import java.util.Map;
import java.util.function.Consumer;

/**
 * Collection of utility methods for working with maps.
 */
public class MapUtils {

    public static void applyLong(Map<String, String> map, String key, Consumer<Long> consumer) {
        String val = map.get(key);
        if (val != null) {
            try {
                consumer.accept(Long.valueOf(val));
            } catch (NumberFormatException e) {
                throw new RuntimeException("Invalid value for key " + key + ": " + val, e);
            }
        }
    }
    public static void applyInt(Map<String, String> map, String key, Consumer<Integer> consumer) {
        String val = map.get(key);
        if (val != null) {
            try {
                consumer.accept(Integer.valueOf(val));
            } catch (NumberFormatException e) {
                throw new RuntimeException("Invalid value for key " + key + ": " + val, e);
            }
        }
    }
}

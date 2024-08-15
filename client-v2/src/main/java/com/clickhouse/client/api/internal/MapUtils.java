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

    public static int getInt(Map<String, String> map, String key) {
        String val = map.get(key);
        if (val != null) {
            try {
                return Integer.parseInt(val);
            } catch (NumberFormatException e) {
                throw new RuntimeException("Invalid value for key " + key + ": " + val, e);
            }
        }
        return 0;
    }

    public static boolean getFlag(Map<String, String> map, String key) {
        String val = map.get(key);
        if (val == null) {
            throw new NullPointerException("Missing value for the key '" + key + "'");
        }
        if (val.equalsIgnoreCase("true")) {
            return true;
        } else if (val.equalsIgnoreCase("false")) {
            return false;
        }

        throw new IllegalArgumentException("Invalid non-boolean value for the key '" + key + "': '" + val + "'");
    }

    public static boolean getFlag(Map<String, ?> p1, Map<String, ?> p2, String key) {
        Object val = p1.get(key);
        if (val == null) {
            val = p2.get(key);
        }
        if (val == null) {
            throw new NullPointerException("Missing value for the key '" + key + "'");
        }

        if (val instanceof Boolean) {
            return (Boolean) val;
        } else if (val instanceof String) {
            String str = (String) val;
            if (str.equalsIgnoreCase("true")) {
                return true;
            } else if (str.equalsIgnoreCase("false")) {
                return false;
            } else {
                throw new IllegalArgumentException("Invalid non-boolean value for the key '" + key + "': '" + val + "'");
            }
        } else {
            throw new IllegalArgumentException("Invalid non-boolean value for the key '" + key + "': '" + val + "'");
        }
    }
}

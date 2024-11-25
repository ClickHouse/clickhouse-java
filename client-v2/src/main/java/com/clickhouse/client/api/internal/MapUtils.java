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

    public static long getLong(Map<String, String> map, String key) {
        String val = map.get(key);
        if (val != null) {
            try {
                return Long.parseLong(val);
            } catch (NumberFormatException e) {
                throw new RuntimeException("Invalid value for key " + key + ": " + val, e);
            }
        }
        return 0;
    }

    /**
     * Get a boolean value from a map.
     *
     * @param map map to get value from
     * @param key key to get value for
     * @return boolean value
     * @throws NullPointerException if the key is missing
     * @throws IllegalArgumentException if the value is not a boolean
     */
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

    /**
     * Get a boolean value from a map.
     * @param map - configuration map
     * @param key - key of the property
     * @param defaultValue - value if not found
     * @return boolean value
     */
    public static boolean getFlag(Map<String, ?> map, String key, boolean defaultValue) {
        Object val = map.get(key);
        if (val == null) {
            return defaultValue;
        }
        if (val instanceof Boolean) {
            return (Boolean) val;
        } else if (val instanceof String) {
            String str = (String) val;
            if (str.equalsIgnoreCase("true") || str.equalsIgnoreCase("1")) {
                return true;
            } else if (str.equalsIgnoreCase("false") || str.equalsIgnoreCase("0")) {
                return false;
            }
        }
        throw new IllegalArgumentException("Invalid non-boolean value for the key '" + key + "': '" + val + "'");
    }

    /**
     * Get a boolean value from a p1, if not found, get from p2.
     *
     * @param p1 - first map
     * @param p2 - second map
     * @param key - key of the property
     * @return boolean value
     * @throws NullPointerException if the key is missing in both maps
     */
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

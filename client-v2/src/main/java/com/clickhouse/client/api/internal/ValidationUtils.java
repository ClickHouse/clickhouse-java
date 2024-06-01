package com.clickhouse.client.api.internal;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class ValidationUtils {

    public static final int TCP_PORT_NUMBER_MAX = (1 << 16) -1;

    public static void checkRange(int value, int min, int max, String name) {
        if (value < min || value > max) {
            throw new SettingsValidationException(name, "\"" + name + "\" must be in range [" + min + ", " + max + "]");
        }
    }

    public static void checkRange(long value, long min, long max, String name) {
        if (value < min || value > max) {
            throw new SettingsValidationException(name, "\"" + name + "\" must be in range [" + min + ", " + max + "]");
        }
    }

    public static void checkPositive(int value, String name) {
        if (value <= 0) {
            throw new SettingsValidationException(name, "\"" + name + "\" must be positive");
        }
    }

    public static void checkNonBlank(String value, String name) {
        if (value == null || value.isEmpty()) {
            throw new SettingsValidationException(name, "\"" + name + "\" must be non-null and non-empty");
        }
    }

    public static void checkNotNull(Object value, String name) {
        if (value == null) {
            throw new SettingsValidationException(name, "\"" + name + "\" must be non-null");
        }
    }

    public static void checkValueFromSet(Object value, String name, Set<?> validValues) {
        if (!validValues.contains(value)) {
            throw new SettingsValidationException(name, "\"" + name + "\" must be one of " + validValues);
        }
    }

    /**
     * Creates a unmodifiable set from the given values.
     * @param values
     * @return
     * @param <T>
     */
    public static <T> Set<T> whiteList(T... values) {
        return Collections.unmodifiableSet(new HashSet<>(Arrays.asList(values)));
    }

    public static class SettingsValidationException extends IllegalArgumentException {
        private static final long serialVersionUID = 1L;

        private final String key;

        public SettingsValidationException(String key, String message) {
            super(message);
            this.key = key;
        }
    }
}

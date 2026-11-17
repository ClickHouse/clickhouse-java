package com.clickhouse.client.api.internal;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.zone.ZoneRules;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.TimeZone;

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
     * Converts a time zone ID to a {@link TimeZone}. Unlike {@link TimeZone#getTimeZone(String)}, which returns
     * GMT for an ID that it does not recognise, this method throws an exception for an unknown or malformed ID.
     * IDs are validated by {@link ZoneId#of(String)}.
     *
     * @param timeZoneId - time zone ID, for example {@code Europe/Berlin}, {@code UTC} or {@code +05:30}
     * @param name - name of the setting, used in the error message
     * @return time zone for the ID
     * @throws SettingsValidationException if the ID is not a valid time zone ID, or if it is a fixed offset
     *                                     with seconds, for example {@code +05:30:45} (on Java 17 and earlier,
     *                                     {@link TimeZone#getTimeZone(ZoneId)} returns GMT for it)
     */
    public static TimeZone toTimeZone(String timeZoneId, String name) {
        ZoneId zoneId;
        try {
            zoneId = ZoneId.of(timeZoneId);
        } catch (DateTimeException e) {
            SettingsValidationException error = new SettingsValidationException(name,
                    "\"" + name + "\" must be a valid time zone ID, but was '" + timeZoneId + "'");
            error.initCause(e);
            throw error;
        }
        String id = zoneId.getId();
        if (id.startsWith("UTC+") || id.startsWith("UTC-") || id.startsWith("UT+") || id.startsWith("UT-")) {
            // TimeZone does not recognise these offset IDs and returns GMT for them. Use the offset itself.
            zoneId = zoneId.normalized();
        }
        ZoneRules rules = zoneId.getRules();
        if (rules.isFixedOffset() && rules.getOffset(Instant.EPOCH).getTotalSeconds() % 60 != 0) {
            // On Java 17 and earlier, TimeZone.getTimeZone returns GMT for an offset with seconds. Reject such
            // offsets on every Java version, so that the result does not depend on the runtime.
            throw new SettingsValidationException(name, "\"" + name
                    + "\" must be a time zone ID with an offset in whole minutes, but was '" + timeZoneId + "'");
        }
        return TimeZone.getTimeZone(zoneId);
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

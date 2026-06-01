package com.clickhouse.client.api.internal;

/**
 * Class containing utility methods used across the client.
 */
public final class ClientUtils {

    private ClientUtils() {}

    /**
     * Checks whether the given string is non-null and contains at least one non-whitespace character.
     *
     * @param str the string to check
     * @return {@code true} if {@code str} is non-null and has at least one non-whitespace character
     */
    public static boolean isNotBlank(String str) {
        return str != null && !str.trim().isEmpty();
    }

    /**
     * Checks whether the given string is null or contains only whitespace.
     *
     * @param str the string to check
     * @return {@code true} if {@code str} is null or trims to an empty string
     */
    public static boolean isBlank(String str) {
        return str == null || str.trim().isEmpty();
    }

    /**
     * Checks whether the given string is null or empty (without trimming).
     *
     * @param str the string to check
     * @return {@code true} if {@code str} is null or has zero length
     */
    public static boolean isNullOrEmpty(String str) {
        return str == null || str.isEmpty();
    }

    /**
     * Returns {@code value} if it is not blank, otherwise returns {@code defaultValue}.
     *
     * @param value        the candidate value
     * @param defaultValue the value to return when {@code value} is null or only whitespace
     * @return {@code value} when non-blank, {@code defaultValue} otherwise
     */
    public static String defaultIfBlank(String value, String defaultValue) {
        return isBlank(value) ? defaultValue : value;
    }

    /**
     * Validates that the given string is non-blank.
     *
     * @param value the value to validate
     * @param name  human-readable name used in the error message
     * @return {@code value} unchanged
     * @throws IllegalArgumentException if {@code value} is null or only whitespace
     */
    public static String requireNonBlank(String value, String name) {
        if (isBlank(value)) {
            throw new IllegalArgumentException("\"" + name + "\" must be non-null and non-blank");
        }
        return value;
    }

    /**
     * Truncates the given string to at most {@code maxLength} characters. A {@code maxLength} of
     * {@code 0} returns an empty string; a negative value throws {@link IllegalArgumentException}.
     *
     * @param value     the string to truncate (may be null)
     * @param maxLength maximum number of characters to keep (must be non-negative)
     * @return the original string when null or already short enough, or a truncated copy otherwise
     * @throws IllegalArgumentException if {@code maxLength} is negative
     */
    public static String truncate(String value, int maxLength) {
        if (maxLength < 0) {
            throw new IllegalArgumentException("maxLength must be non-negative, got " + maxLength);
        }
        if (value == null || value.length() <= maxLength) {
            return value;
        }
        return value.substring(0, maxLength);
    }

    /**
     * Null-safe, case-insensitive string comparison.
     *
     * @param a first string (may be null)
     * @param b second string (may be null)
     * @return {@code true} if both are null or if they are equal ignoring case
     */
    public static boolean equalsIgnoreCase(String a, String b) {
        return a == null ? b == null : a.equalsIgnoreCase(b);
    }
}

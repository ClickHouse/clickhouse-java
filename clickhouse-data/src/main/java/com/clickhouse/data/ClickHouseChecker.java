package com.clickhouse.data;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Locale;

/**
 * Utility class for validation.
 */

@Deprecated
public final class ClickHouseChecker {
    private static final String DEFAULT_NAME = "value";
    private static final String ERR_SHOULD_BETWEEN = "%s(%s) should be between %s and %s inclusive of both values";
    private static final String ERR_SHOULD_BETWEEN_EXCLUSIVE = "%s(%s) should be between %s and %s exclusive of both values";
    private static final String ERR_SHOULD_GE = "%s(%s) should NOT be less than %s";

    static final IllegalArgumentException newException(String format, Object... args) {
        return new IllegalArgumentException(String.format(Locale.ROOT, format, args));
    }

    /**
     * Checks if the given {@code value} is between {@code minValue} and
     * {@code maxValue} inclusive and throws a customized
     * {@link IllegalArgumentException} if it is NOT.
     *
     * @param value    the value to check
     * @param minValue minimum value to compare with
     * @param maxValue maximum value to compare with
     * @return the exact same value
     * @throws IllegalArgumentException if the {@code value} is NOT between
     *                                  {@code minValue} and {@code maxValue}
     */
    public static int between(int value, int minValue, int maxValue) {
        return between(value, DEFAULT_NAME, minValue, maxValue);
    }

    /**
     * Checks if the given {@code value} is between {@code minValue} and
     * {@code maxValue} inclusive and throws a customized
     * {@link IllegalArgumentException} if it is NOT.
     *
     * @param value    the value to check
     * @param minValue minimum value to compare with
     * @param maxValue maximum value to compare with
     * @return the exact same value
     * @throws IllegalArgumentException if the {@code value} is NOT between
     *                                  {@code minValue} and {@code maxValue}
     */
    public static long between(long value, long minValue, long maxValue) {
        return between(value, DEFAULT_NAME, minValue, maxValue);
    }

    /**
     * Checks if the given {@code value} is between {@code minValue} and
     * {@code maxValue} inclusive and throws a customized
     * {@link IllegalArgumentException} if it is NOT.
     *
     * @param value    the value to check
     * @param name     name of the value
     * @param minValue minimum value to compare with
     * @param maxValue maximum value to compare with
     * @return the exact same value
     * @throws IllegalArgumentException if the {@code value} is NOT between
     *                                  {@code minValue} and {@code maxValue}
     */
    public static int between(byte value, String name, byte minValue, byte maxValue) {
        if (value < minValue || value > maxValue) {
            throw newException(ERR_SHOULD_BETWEEN, name, value, minValue, maxValue);
        }

        return value;
    }

    /**
     * Checks if the given {@code value} is between {@code minValue} and
     * {@code maxValue} inclusive and throws a customized
     * {@link IllegalArgumentException} if it is NOT.
     *
     * @param value    the value to check
     * @param name     name of the value
     * @param minValue minimum value to compare with
     * @param maxValue maximum value to compare with
     * @return the exact same value
     * @throws IllegalArgumentException if the {@code value} is NOT between
     *                                  {@code minValue} and {@code maxValue}
     */
    public static int between(int value, String name, int minValue, int maxValue) {
        if (value < minValue || value > maxValue) {
            throw newException(ERR_SHOULD_BETWEEN, name, value, minValue, maxValue);
        }

        return value;
    }

    /**
     * Checks if the given {@code value} is between {@code minValue} and
     * {@code maxValue} inclusive and throws a customized
     * {@link IllegalArgumentException} if it is NOT.
     *
     * @param value    the value to check
     * @param name     name of the value
     * @param minValue minimum value to compare with
     * @param maxValue maximum value to compare with
     * @return the exact same value
     * @throws IllegalArgumentException if the {@code value} is NOT between
     *                                  {@code minValue} and {@code maxValue}
     */
    public static long between(long value, String name, long minValue, long maxValue) {
        if (value < minValue || value > maxValue) {
            throw newException(ERR_SHOULD_BETWEEN, name, value, minValue, maxValue);
        }

        return value;
    }

    /**
     * Checks if the given {@code value} is between {@code minValue} and
     * {@code maxValue} inclusive and throws a customized
     * {@link IllegalArgumentException} if it is NOT.
     *
     * @param value    the value to check
     * @param minValue minimum value to compare with
     * @param maxValue maximum value to compare with
     * @return the exact same value
     * @throws IllegalArgumentException if the {@code value} is NOT between
     *                                  {@code minValue} and {@code maxValue}
     */
    public static BigInteger between(BigInteger value, BigInteger minValue, BigInteger maxValue) {
        return between(value, DEFAULT_NAME, minValue, maxValue);
    }

    /**
     * Checks if the given {@code value} is between {@code minValue} and
     * {@code maxValue} inclusive and throws a customized
     * {@link IllegalArgumentException} if it is NOT.
     *
     * @param value    the value to check
     * @param name     name of the value
     * @param minValue minimum value to compare with
     * @param maxValue maximum value to compare with
     * @return the exact same value
     * @throws IllegalArgumentException if the {@code value} is NOT between
     *                                  {@code minValue} and {@code maxValue}
     */
    public static BigInteger between(BigInteger value, String name, BigInteger minValue, BigInteger maxValue) {
        if (value.compareTo(minValue) < 0 || value.compareTo(maxValue) > 0) {
            throw newException(ERR_SHOULD_BETWEEN, name, value, minValue, maxValue);
        }

        return value;
    }

    /**
     * Checks if the given {@code value} is between {@code minValue} and
     * {@code maxValue} *exclusive* and throws a customized
     * {@link IllegalArgumentException} if it is NOT.
     *
     * @param value    the value to check
     * @param minValue minimum value to compare with
     * @param maxValue maximum value to compare with
     * @return the exact same value
     * @throws IllegalArgumentException if the {@code value} is NOT between
     *                                  {@code minValue} and {@code maxValue}
     */
    public static BigDecimal between(BigDecimal value, BigDecimal minValue, BigDecimal maxValue) {
        return between(value, DEFAULT_NAME, minValue, maxValue);
    }

    /**
     * Checks if the given {@code value} is between {@code minValue} and
     * {@code maxValue} *exclusive* and throws a customized
     * {@link IllegalArgumentException} if it is NOT.
     *
     * @param value    the value to check
     * @param name     name of the value
     * @param minValue minimum value to compare with
     * @param maxValue maximum value to compare with
     * @return the exact same value
     * @throws IllegalArgumentException if the {@code value} is NOT between
     *                                  {@code minValue} and {@code maxValue}
     */
    public static BigDecimal between(BigDecimal value, String name, BigDecimal minValue, BigDecimal maxValue) {
        if (value.compareTo(minValue) <= 0 || value.compareTo(maxValue) >= 0) {
            throw newException(ERR_SHOULD_BETWEEN_EXCLUSIVE, name, value, minValue, maxValue);
        }

        return value;
    }

    /**
     * Checks if the given string is {@code null} or empty.
     *
     * @param value the string to check
     * @return true if the string is null or empty; false otherwise
     */
    public static boolean isNullOrEmpty(CharSequence value) {
        return value == null || value.length() == 0;
    }

    /**
     * Checks if the given string is null, empty string or a string only contains
     * white spaces.
     *
     * @param value the string to check
     * @return true if the string is null, empty or blank； false otherwise
     */
    public static boolean isNullOrBlank(CharSequence value) {
        if (isNullOrEmpty(value)) {
            return true;
        }

        for (int i = 0, len = value.length(); i < len; i++) {
            if (!Character.isWhitespace(value.charAt(i))) {
                return false;
            }
        }

        return true;
    }

    /**
     * Checks if the given string is not {@code null}, empty or blank and throws a
     * customized {@link IllegalArgumentException} if it is.
     *
     * @param value the string to check
     * @param name  name of the string
     * @return the exact same string
     * @throws IllegalArgumentException if the string is null, empty or blank
     */
    public static String nonBlank(String value, String name) {
        if (isNullOrBlank(value)) {
            throw newException("%s cannot be null, empty or blank string", name);
        }

        return value;
    }

    /**
     * Checks if the given string is neither {@code null} nor empty and throws a
     * customized {@link IllegalArgumentException} if it is.
     *
     * @param value the string to check
     * @param name  name of the string
     * @return the exact same string
     * @throws IllegalArgumentException if the string is null or empty
     */
    public static String nonEmpty(String value, String name) {
        if (isNullOrEmpty(value)) {
            throw newException("%s cannot be null or empty string", name);
        }

        return value;
    }

    /**
     * Checks if the given object is NOT {@code null} and throws a customized
     * {@link IllegalArgumentException} if it is.
     *
     * @param <T>   type of the object
     * @param value the object
     * @param name  name of the object
     * @return the exact same object
     * @throws IllegalArgumentException if the object is null
     */
    public static final <T> T nonNull(T value, String name) {
        if (value == null) {
            throw newException("Non-null %s is required", name);
        }

        return value;
    }

    /**
     * Checks if the given {@code value} is NOT less than {@code minValue} and
     * throws a customized {@link IllegalArgumentException} if it is.
     *
     * @param value    the value to check
     * @param name     name of the value
     * @param minValue minimum value to compare with
     * @return the exact same value
     * @throws IllegalArgumentException if the {@code value} is less than
     *                                  {@code minValue}
     */
    public static int notLessThan(int value, String name, int minValue) {
        if (value < minValue) {
            throw newException(ERR_SHOULD_GE, name, value, minValue);
        }

        return value;
    }

    /**
     * Checks if the given {@code value} is NOT less than {@code minValue} and
     * throws a customized {@link IllegalArgumentException} if it is.
     *
     * @param value    the value to check
     * @param name     name of the value
     * @param minValue minimum value to compare with
     * @return the exact same value
     * @throws IllegalArgumentException if the {@code value} is less than
     *                                  {@code minValue}
     */
    public static long notLessThan(long value, String name, long minValue) {
        if (value < minValue) {
            throw newException(ERR_SHOULD_GE, name, value, minValue);
        }

        return value;
    }

    /**
     * Checks if the given {@code value} is NOT less than {@code minValue} and
     * throws a customized {@link IllegalArgumentException} if it is.
     *
     * @param value    the value to check
     * @param name     name of the value
     * @param minValue minimum value to compare with
     * @return the exact same value
     * @throws IllegalArgumentException if the {@code value} is less than
     *                                  {@code minValue}
     */
    public static BigInteger notLessThan(BigInteger value, String name, BigInteger minValue) {
        if (value.compareTo(minValue) < 0) {
            throw newException(ERR_SHOULD_GE, name, value, minValue);
        }

        return value;
    }

    /**
     * Checks if length of the given byte array is NOT greater than {@code length}
     * and throws a customized {@link IllegalArgumentException} if it is.
     *
     * @param value     the byte array to check
     * @param name      name of the byte array
     * @param maxLength maximum length of the byte array
     * @return the exact same byte array
     * @throws IllegalArgumentException if length of the byte array is greater than
     *                                  {@code maxlength}
     */
    public static byte[] notLongerThan(byte[] value, String name, int maxLength) {
        int length = value == null ? 0 : value.length;
        if (length > maxLength) {
            throw newException("length of byte array %s is %d, which should NOT longer than %d", name, length,
                    maxLength);
        }

        return value;
    }

    private ClickHouseChecker() {
    }
}

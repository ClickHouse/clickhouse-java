package com.clickhouse.client.config;

import java.util.Optional;

/**
 * This defines a configuration option. To put it in a nutshell, an option is
 * composed of key, default value(which implies type of the value) and
 * description.
 */
public interface ClickHouseConfigOption {
    /**
     * Converts given string to a typed value.
     *
     * @param <T>   type of the value
     * @param value value in string format
     * @param clazz class of the value
     * @return typed value
     */
    static <T> T fromString(String value, Class<T> clazz) {
        if (value == null || clazz == null) {
            throw new IllegalArgumentException("Non-null value and class are required");
        }

        if (clazz == int.class || clazz == Integer.class) {
            return clazz.cast(Integer.valueOf(value));
        }
        if (clazz == long.class || clazz == Long.class) {
            return clazz.cast(Long.valueOf(value));
        }
        if (clazz == boolean.class || clazz == Boolean.class) {
            final Boolean boolValue;
            if ("1".equals(value) || "0".equals(value)) {
                boolValue = "1".equals(value);
            } else {
                boolValue = Boolean.valueOf(value);
            }
            return clazz.cast(boolValue);
        }

        return clazz.cast(value);
    }

    /**
     * Gets default value of the option.
     *
     * @return default value of the option
     */
    Object getDefaultValue();

    /**
     * Gets default value from environment variable. By default the environment
     * variable is named as {@link #getPrefix()} + "_" + {@link #name()} in upper
     * case.
     *
     * @return default value defined in environment variable
     */
    default Optional<String> getDefaultValueFromEnvVar() {
        String prefix = getPrefix().toUpperCase();
        String optionName = name();
        int length = optionName.length();
        return Optional.ofNullable(System.getenv(new StringBuilder(length + prefix.length() + 1).append(prefix)
                .append('_').append(optionName.toUpperCase()).toString()));
    }

    /**
     * Gets default value from system property. By default the system property is
     * named as {@link #getPrefix()} + "_" + {@link #name()} in lower case.
     *
     * @return default value defined in system property
     */
    default Optional<String> getDefaultValueFromSysProp() {
        String prefix = getPrefix().toLowerCase();
        String optionName = name();
        int length = optionName.length();
        return Optional.ofNullable(System.getProperty(new StringBuilder(length + prefix.length() + 1).append(prefix)
                .append('_').append(optionName.toLowerCase()).toString()));
    }

    /**
     * Gets description of the option.
     *
     * @return description of the option
     */
    String getDescription();

    /**
     * Gets effective default value by considering default value defined in system
     * property and environment variable. It's same as {@link #getDefaultValue()} if
     * no system property and environment variable defined.
     *
     * @return effective default value
     */
    default Object getEffectiveDefaultValue() {
        Optional<String> value = getDefaultValueFromEnvVar();

        if (!value.isPresent() || value.get().isEmpty()) {
            value = getDefaultValueFromSysProp();
        }

        if (!value.isPresent() || value.get().isEmpty()) {
            return getDefaultValue();
        }

        return fromString(value.get(), getValueType());
    }

    /**
     * Gets effective value by considering default value defined in system property
     * and environment variable. It's same as {@link #getDefaultValue()} if the
     * given value is null and no system property and environment variable defined.
     *
     * @param <T>   type of the value
     * @param value default value
     * @return effective value
     */
    default <T> T getEffectiveValue(T value) {
        @SuppressWarnings("unchecked")
        T result = value == null ? (T) getEffectiveDefaultValue() : value;
        return result;
    }

    /**
     * Gets key of the option.
     *
     * @return key of the option
     */
    String getKey();

    /**
     * Gets prefix of environment variable and system property.
     *
     * @return prefix of environment variable and system property
     */
    default String getPrefix() {
        return "CHC";
    }

    /**
     * Gets value type of the option.
     *
     * @return value type of the option, defaults to String
     */
    Class<?> getValueType();

    /**
     * Gets name of the option.
     *
     * @return name of the option
     */
    String name();
}

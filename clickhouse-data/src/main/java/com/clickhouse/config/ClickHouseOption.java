package com.clickhouse.config;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;

/**
 * This defines a configuration option. To put it in a nutshell, an option is
 * composed of key, default value(which implies type of the value) and
 * description.
 */
@Deprecated
public interface ClickHouseOption extends Serializable {
    /**
     * Converts given string to key value pairs.
     * 
     * @param str string
     * @return non-null key value pairs
     */
    static Map<String, String> toKeyValuePairs(String str) {
        if (str == null || str.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, String> map = new LinkedHashMap<>();
        String key = null;
        StringBuilder builder = new StringBuilder();
        for (int i = 0, len = str.length(); i < len; i++) {
            char ch = str.charAt(i);
            if (ch == '\\' && i + 1 < len) {
                ch = str.charAt(++i);
                builder.append(ch);
                continue;
            }

            if (Character.isWhitespace(ch)) {
                if (builder.length() > 0) {
                    builder.append(ch);
                }
            } else if (ch == '=' && key == null) {
                key = builder.toString().trim();
                builder.setLength(0);
            } else if (ch == ',' && key != null) {
                String value = builder.toString().trim();
                builder.setLength(0);
                if (!key.isEmpty() && !value.isEmpty()) {
                    map.put(key, value);
                }
                key = null;
            } else {
                builder.append(ch);
            }
        }

        if (key != null && builder.length() > 0) {
            String value = builder.toString().trim();
            if (!key.isEmpty() && !value.isEmpty()) {
                map.put(key, value);
            }
        }

        return Collections.unmodifiableMap(map);
    }

    /**
     * Converts given string to a typed value.
     *
     * @param <T>   type of the value
     * @param value value in string format
     * @param clazz non-null class of the value
     * @return non-null typed value
     */
    @SuppressWarnings("unchecked")
    static <T extends Serializable> T fromString(String value, Class<T> clazz) {
        if (clazz == null) {
            throw new IllegalArgumentException("Non-null value type is required");
        } else if (value == null) {
            value = "";
        }

        T result;
        if (clazz == boolean.class || clazz == Boolean.class) {
            final Boolean boolValue;
            if (value.isEmpty()) {
                boolValue = Boolean.FALSE;
            } else if (value.length() == 1) {
                boolValue = "1".equals(value);
            } else {
                boolValue = Boolean.valueOf(value);
            }
            result = (T) boolValue;
        } else if (byte.class == clazz || Byte.class == clazz) {
            result = (T) (value.isEmpty() ? Byte.valueOf((byte) 0) : Byte.valueOf(value));
        } else if (short.class == clazz || Short.class == clazz) {
            result = (T) (value.isEmpty() ? Short.valueOf((short) 0) : Short.valueOf(value));
        } else if (int.class == clazz || Integer.class == clazz) {
            result = (T) (value.isEmpty() ? Integer.valueOf(0) : Integer.valueOf(value));
        } else if (long.class == clazz || Long.class == clazz) {
            result = (T) (value.isEmpty() ? Long.valueOf(0L) : Long.valueOf(value));
        } else if (float.class == clazz || Float.class == clazz) {
            result = (T) (value.isEmpty() ? Float.valueOf(0F) : Float.valueOf(value));
        } else if (double.class == clazz || Double.class == clazz) {
            result = (T) (value.isEmpty() ? Double.valueOf(0D) : Double.valueOf(value));
        } else if (Enum.class.isAssignableFrom(clazz)) {
            Enum enumValue = null;
            try {
                enumValue = Enum.valueOf((Class<? extends Enum>) clazz, value);
            } catch (IllegalArgumentException exp) {
                for (Enum<?> e : ((Class<? extends Enum>) clazz).getEnumConstants()) {
                    if (e.name().equalsIgnoreCase(value)) {
                        enumValue = e;
                        break;
                    }
                }
            }
            if (enumValue == null) {
                throw new IllegalArgumentException("No enum constant " + clazz.getCanonicalName() + "." + value);
            } else {
                result = (T) enumValue;
            }
        } else if (Map.class.isAssignableFrom(clazz)) { // treat as Map<String, String> & Serializable
            result = (T) toKeyValuePairs(value);
        } else if (TimeZone.class.isAssignableFrom(clazz)) {
            result = (T) TimeZone.getTimeZone(value);
        } else {
            result = clazz.cast(value);
        }
        return result;
    }

    /**
     * Converts given string to typed value. When {@code value} is null or blank,
     * {@code defaultValue} will be returned.
     *
     * @param <T>          type of the value
     * @param value        value in string format
     * @param defaultValue non-null default value
     * @return non-null typed value
     */
    @SuppressWarnings("unchecked")
    static <T extends Serializable> T fromString(String value, T defaultValue) {
        if (defaultValue == null) {
            throw new IllegalArgumentException("Non-null default value is required");
        }
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }

        return (T) fromString(value, defaultValue.getClass());
    }

    /**
     * Gets default value of the option.
     *
     * @return default value of the option
     */
    Serializable getDefaultValue();

    /**
     * Gets trimmed default value from environment variable. By default the
     * environment variable is named as {@link #getPrefix()} + "_" + {@link #name()}
     * in upper case.
     *
     * @return trimmed default value defined in environment variable
     */
    default Optional<String> getDefaultValueFromEnvVar() {
        String value = System.getenv(getEnvironmentVariable());
        if (value != null) {
            value = value.trim();
        }
        return Optional.ofNullable(value);
    }

    /**
     * Gets trimmed default value from system property. By default the system
     * property is named as {@link #getPrefix()} + "_" + {@link #name()} in lower
     * case.
     *
     * @return trimmed default value defined in system property
     */
    default Optional<String> getDefaultValueFromSysProp() {
        String value = System.getProperty(getSystemProperty());
        if (value != null) {
            value = value.trim();
        }
        return Optional.ofNullable(value);
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
    default Serializable getEffectiveDefaultValue() {
        Optional<String> value = getDefaultValueFromEnvVar();

        if (!value.isPresent() || value.get().isEmpty()) {
            value = getDefaultValueFromSysProp();
        }

        if (!value.isPresent() || value.get().isEmpty()) {
            return getDefaultValue();
        }

        return fromString(value.get(), (Class<? extends Serializable>) getValueType());
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
     * Gets environment variable for the option.
     *
     * @return environment variable
     */
    default String getEnvironmentVariable() {
        String name = name().toUpperCase(Locale.ROOT);
        String prefix = getPrefix().toUpperCase(Locale.ROOT);
        return new StringBuilder(prefix.length() + name.length() + 1).append(prefix).append('_').append(name)
                .toString();
    }

    /**
     * Gets system property for the option.
     *
     * @return system property
     */
    default String getSystemProperty() {
        String name = name().toLowerCase(Locale.ROOT);
        String prefix = getPrefix().toLowerCase(Locale.ROOT);
        return new StringBuilder(prefix.length() + name.length() + 1).append(prefix).append('_').append(name)
                .toString();
    }

    /**
     * Gets value type of the option.
     *
     * @return value type of the option, defaults to String
     */
    Class<? extends Serializable> getValueType();

    /**
     * Checks whether the option is sensitive or not. A sensitive option should be
     * used carefully and be masked in logging for security reason.
     *
     * @return true if the option is sensitive; false otherwise
     */
    boolean isSensitive();

    /**
     * Gets name of the option.
     *
     * @return name of the option
     */
    String name();
}

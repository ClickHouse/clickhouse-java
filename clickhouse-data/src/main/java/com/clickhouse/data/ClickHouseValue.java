package com.clickhouse.data;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Serializable;
import java.io.StringReader;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Wrapper of a value returned from ClickHouse. It could be as simple as one
 * single byte or in a complex structure like nested arrays. It also provides
 * convenient methods for type conversion(e.g. use {@link #asDateTime()} to
 * convert an integer to {@link java.time.LocalDateTime}).
 */
public interface ClickHouseValue extends Serializable {
    /**
     * Create a customized exception for unsupported type conversion.
     *
     * @param from type to convert from
     * @param to   type to convert to
     * @return customized exception
     */
    default UnsupportedOperationException newUnsupportedException(String from, String to) {
        return new UnsupportedOperationException(
                ClickHouseUtils.format("Converting [%s] to [%s] is not supported", from, to));
    }

    /**
     * Gets a shallow copy of this value object. Same as {@code copy(false)}.
     *
     * @return shallow copy of this value object
     */
    default ClickHouseValue copy() {
        return copy(false);
    }

    /**
     * Gets a copy of this value object.
     *
     * @param deep true to create a deep copy; false for a shallow copy
     * @return copy of this value object
     */
    ClickHouseValue copy(boolean deep);

    /**
     * Checks if the value is either positive or negative infinity as defined in
     * {@link Double}.
     *
     * @return true if it's infinity; false otherwise
     */
    default boolean isInfinity() {
        double value = asDouble();
        return value == Double.NEGATIVE_INFINITY || value == Double.POSITIVE_INFINITY;
    }

    /**
     * Checks if the value is Not-a-Number (NaN).
     *
     * @return true if the value is NaN; false otherwise
     */
    default boolean isNaN() {
        return Double.isNaN(asDouble());
    }

    /**
     * Checks whether the value is nullable. This always returns {@code false} for
     * nested value type.
     *
     * @return true if the value is nullable; false otherwise
     */
    default boolean isNullable() {
        return true;
    }

    /**
     * Checks if the value is null, or empty for non-null types like Array, Tuple
     * and Map.
     *
     * <p>
     * Please pay attention that only nullability will be considered for String,
     * meaning this method will return {@code false} for an empty string. This is
     * because String is treated as value-based type instead of a container like
     * Array.
     *
     * @return true if the value is null or empty; false otherwise
     */
    boolean isNullOrEmpty();

    /**
     * Gets value as an object array.
     *
     * @return non-null object array
     */
    default Object[] asArray() {
        if (isNullOrEmpty()) {
            return ClickHouseValues.EMPTY_OBJECT_ARRAY;
        }

        return new Object[] { asObject() };
    }

    /**
     * Gets value as an array.
     *
     * @param <T>   type of the element
     * @param clazz class of the element
     * @return non-null array
     */
    @SuppressWarnings("unchecked")
    default <T> T[] asArray(Class<T> clazz) {
        if (isNullOrEmpty()) {
            return (T[]) ClickHouseValues.EMPTY_OBJECT_ARRAY;
        }

        T[] array = (T[]) Array.newInstance(ClickHouseChecker.nonNull(clazz, ClickHouseValues.TYPE_CLASS), 1);
        array[0] = asObject(clazz);
        return array;
    }

    /**
     * Gets value as byte stream. It's caller's responsibility to close the stream
     * at the end of reading.
     *
     * @return non-null byte stream for reading
     */
    default InputStream asByteStream() {
        return isNullOrEmpty() ? ClickHouseInputStream.empty() : new ByteArrayInputStream(new byte[] { asByte() });
    }

    /**
     * Gets value as character stream. It's caller's responsibility to close the
     * tream at the end of reading.
     *
     * @return non-null character stream for reading
     */
    default Reader asCharacterStream() {
        // Reader.nullReader() requires JDK 11+
        return new StringReader(isNullOrEmpty() ? "" : asString());
    }

    /**
     * Gets value as boolean.
     *
     * @return boolean value
     */
    default boolean asBoolean() {
        return !isNullOrEmpty() && ClickHouseChecker.between(asByte(), 0, 1) == 1;
    }

    /**
     * Gets value as character.
     *
     * @return character value
     */
    default char asCharacter() {
        return (char) asShort();
    }

    /**
     * Gets value as byte.
     *
     * @return byte value
     */
    byte asByte();

    /**
     * Gets value as short.
     *
     * @return short value
     */
    short asShort();

    /**
     * Gets value as integer.
     *
     * @return integer value
     */
    int asInteger();

    /**
     * Gets value as long.
     *
     * @return long value
     */
    long asLong();

    /**
     * Gets value as {@link java.math.BigInteger}.
     *
     * @return big integer, could be null
     */
    BigInteger asBigInteger();

    /**
     * Gets value as float.
     *
     * @return float value
     */
    float asFloat();

    /**
     * Gets value as double.
     *
     * @return double value
     */
    double asDouble();

    /**
     * Gets value as {@link java.math.BigDecimal}, using default scale(usually 0).
     *
     * @return big decimal, could be null
     */
    default BigDecimal asBigDecimal() {
        return asBigDecimal(0);
    }

    /**
     * Gets value as {@link java.math.BigDecimal}.
     *
     * @param scale scale of the decimal
     * @return big decimal, could be null
     */
    BigDecimal asBigDecimal(int scale);

    /**
     * Gets value as {@link java.time.LocalDate}.
     *
     * @return date, could be null
     */
    default LocalDate asDate() {
        if (isNullOrEmpty()) {
            return null;
        }

        return LocalDate.ofEpochDay(asLong());
    }

    /**
     * Gets value as {@link java.time.LocalTime}.
     *
     * @return time, could be null
     */
    default LocalTime asTime() {
        return asTime(0);
    }

    /**
     * Gets value as {@link java.time.LocalTime}.
     *
     * @param scale scale of the date time, between 0 (second) and 9 (nano second)
     * @return time, could be null
     */
    default LocalTime asTime(int scale) {
        if (isNullOrEmpty()) {
            return null;
        }

        return asDateTime(scale).toLocalTime();
    }

    /**
     * Gets value as {@link java.time.LocalDateTime}, using default scale(usually
     * 0).
     *
     * @return date time, could be null
     */
    default LocalDateTime asDateTime() {
        return asDateTime(0);
    }

    /**
     * Gets value as {@link java.time.Instant}, using default scale(usually
     * 0).
     *
     * @return date time, could be null
     */
    default Instant asInstant() {
        return asInstant(0);
    }

    /**
     * Gets value as {@link java.time.OffsetDateTime}, using default scale(usually
     * 0).
     *
     * @return date time, could be null
     */
    default OffsetDateTime asOffsetDateTime() {
        return asOffsetDateTime(0);
    }

    /**
     * Gets value as {@link java.time.ZonedDateTime}, using default scale(usually
     * 0).
     *
     * @return date time, could be null
     */
    default ZonedDateTime asZonedDateTime() {
        return asZonedDateTime(0);
    }

    /**
     * Gets value as {@link java.time.LocalDateTime}.
     *
     * @param scale scale of the date time, between 0 (second) and 9 (nano second)
     * @return date time, could be null
     */
    default LocalDateTime asDateTime(int scale) {
        if (isNullOrEmpty()) {
            return null;
        }

        return ClickHouseValues
                .convertToDateTime(asBigDecimal(ClickHouseChecker.between(scale, ClickHouseValues.PARAM_SCALE, 0, 9)));
    }

    /**
     * Gets value as {@link java.time.Instant}.
     *
     * @param scale scale of the date time, between 0 (second) and 9 (nano second)
     * @return instant, could be null
     */
    default Instant asInstant(int scale) {
        if (isNullOrEmpty()) {
            return null;
        }

        return ClickHouseValues
                .convertToInstant(asBigDecimal(ClickHouseChecker.between(scale, ClickHouseValues.PARAM_SCALE, 0, 9)));
    }

    /**
     * Gets value as {@link java.time.OffsetDateTime}.
     *
     * @param scale scale of the date time, between 0 (second) and 9 (nano second)
     * @return date time, could be null
     */
    default OffsetDateTime asOffsetDateTime(int scale) {
        if (isNullOrEmpty()) {
            return null;
        }

        return asDateTime(scale).atOffset(ZoneOffset.UTC);
    }

    /**
     * Gets value as {@link java.time.ZonedDateTime}.
     *
     * @param scale scale of the date time, between 0 (second) and 9 (nano second)
     * @return date time, could be null
     */
    default ZonedDateTime asZonedDateTime(int scale) {
        if (isNullOrEmpty()) {
            return null;
        }

        return asDateTime(scale).atZone(ClickHouseValues.UTC_ZONE);
    }

    /**
     * Gets value as enum.
     *
     * @param <T>      type of the enum
     * @param enumType enum class
     * @return enum, could be null
     */
    default <T extends Enum<T>> T asEnum(Class<T> enumType) {
        if (isNullOrEmpty()) {
            return null;
        }

        int value = asInteger();
        for (T t : ClickHouseChecker.nonNull(enumType, ClickHouseValues.TYPE_CLASS).getEnumConstants()) {
            if (t.ordinal() == value) {
                return t;
            }
        }

        throw new IllegalArgumentException(
                ClickHouseUtils.format("Ordinal[%d] not found in %s", value, enumType.getName()));
    }

    /**
     * Gets value as {@link java.net.Inet4Address}.
     *
     * @return IPv4 address, could be null
     */
    default Inet4Address asInet4Address() {
        if (isNullOrEmpty()) {
            return null;
        }

        return ClickHouseValues.convertToIpv4(asInteger());
    }

    /**
     * Gets value as {@link java.net.Inet6Address}.
     *
     * @return IPv6 address, could be null
     */
    default Inet6Address asInet6Address() {
        if (isNullOrEmpty()) {
            return null;
        }

        return ClickHouseValues.convertToIpv6(asBigInteger());
    }

    /**
     * Gets value as a map.
     *
     * @return non-null map value
     */
    default Map<Object, Object> asMap() {
        if (isNullOrEmpty()) {
            return Collections.emptyMap();
        }

        Map<Object, Object> map = new LinkedHashMap<>();
        int index = 1;
        for (Object v : asArray()) {
            map.put(index++, v);
        }

        // why not use Collections.unmodifiableMap(map) here?
        return map;
    }

    /**
     * Gets value as a map.
     *
     * @param <K>        type of key
     * @param <V>        type of value
     * @param keyClass   non-null class of key
     * @param valueClass non-null class of value
     * @return non-null map value
     */
    default <K, V> Map<K, V> asMap(Class<K> keyClass, Class<V> valueClass) {
        if (isNullOrEmpty()) {
            return Collections.emptyMap();
        }

        ClickHouseChecker.nonNull(keyClass, "keyClass");
        Map<K, V> map = new LinkedHashMap<>();
        int index = 1;
        for (V v : asArray(valueClass)) {
            map.put(keyClass.cast(index++), v);
        }
        // why not use Collections.unmodifiableMap(map) here?
        return map;
    }

    /**
     * Gets value as an object.
     *
     * @return an object representing the value, could be null
     */
    Object asObject();

    /**
     * Gets raw value as an object. This method is probably only useful for
     * {@code String} as it can be either a byte array or text.
     * 
     * @return an object representing the raw value, could be null
     */
    default Object asRawObject() {
        return asObject();
    }

    /**
     * Gets value as a typed object.
     *
     * @param <T>   type of the object
     * @param <E>   type of the enum
     * @param clazz class of the object
     * @return a typed object representing the value, could be null
     */
    default <T, E extends Enum<E>> T asObject(Class<T> clazz) {
        if (clazz == null || (isNullable() && isNullOrEmpty())) {
            return null;
        } else if (clazz == boolean.class || clazz == Boolean.class) {
            return clazz.cast(asBoolean());
        } else if (clazz == byte.class || clazz == Byte.class) {
            return clazz.cast(asByte());
        } else if (clazz == char.class || clazz == Character.class) {
            return clazz.cast(asCharacter());
        } else if (clazz == short.class || clazz == Short.class) {
            return clazz.cast(asShort());
        } else if (clazz == int.class || clazz == Integer.class) {
            return clazz.cast(asInteger());
        } else if (clazz == long.class || clazz == Long.class) {
            return clazz.cast(asLong());
        } else if (clazz == float.class || clazz == Float.class) {
            return clazz.cast(asFloat());
        } else if (clazz == double.class || clazz == Double.class) {
            return clazz.cast(asDouble());
        } else if (clazz == String.class) {
            return clazz.cast(asString());
        } else if (clazz == LocalDate.class) {
            return clazz.cast(asDate());
        } else if (clazz == LocalDateTime.class) {
            return clazz.cast(asDateTime());
        } else if (clazz == Instant.class) {
            return clazz.cast(asInstant());
        } else if (clazz == OffsetDateTime.class) {
            return clazz.cast(asOffsetDateTime());
        } else if (clazz == ZonedDateTime.class) {
            return clazz.cast(asZonedDateTime());
        } else if (clazz == LocalTime.class) {
            return clazz.cast(asTime());
        } else if (clazz == BigInteger.class) {
            return clazz.cast(asBigInteger());
        } else if (clazz == BigDecimal.class) {
            return clazz.cast(asBigDecimal());
        } else if (clazz == Inet4Address.class) {
            return clazz.cast(asInet4Address());
        } else if (clazz == Inet6Address.class) {
            return clazz.cast(asInet6Address());
        } else if (clazz == UUID.class) {
            return clazz.cast(asUuid());
        } else if (Array.class.isAssignableFrom(clazz)) {
            return clazz.cast(asArray());
        } else if (List.class.isAssignableFrom(clazz)) {
            return clazz.cast(asTuple());
        } else if (Map.class.isAssignableFrom(clazz)) {
            return clazz.cast(asMap());
        } else if (Enum.class.isAssignableFrom(clazz)) {
            return clazz.cast(asEnum((Class<E>) clazz));
        } else {
            return clazz.cast(asObject());
        }
    }

    /**
     * Gets binary value as byte array.
     *
     * @return byte array which could be null
     */
    default byte[] asBinary() {
        return asBinary(0, null);
    }

    /**
     * Gets binary value as fixed length byte array.
     *
     * @param length byte length of value, 0 or negative number means no limit
     * @return byte array which could be null
     */
    default byte[] asBinary(int length) {
        return asBinary(length, null);
    }

    /**
     * Gets binary value as byte array.
     *
     * @param charset charset, null is same as default(UTF-8)
     * @return byte array which could be null
     */
    default byte[] asBinary(Charset charset) {
        return asBinary(0, charset);
    }

    /**
     * Gets binary value as byte array.
     *
     * @param length  byte length of value, 0 or negative number means no limit
     * @param charset charset, null is same as default(UTF-8)
     * @return byte array which could be null
     */
    default byte[] asBinary(int length, Charset charset) {
        if (isNullOrEmpty()) {
            return null;
        }

        byte[] bytes = asString().getBytes(charset == null ? StandardCharsets.UTF_8 : charset);
        if (bytes.length < length) {
            bytes = Arrays.copyOf(bytes, length);
        }

        return bytes;
    }

    /**
     * Gets value as unbounded string, using default charset(usually UTF-8).
     *
     * @return string value, could be null
     */
    default String asString() {
        if (isNullOrEmpty()) {
            return null;
        }

        return String.valueOf(asObject());
    }

    /**
     * Gets value as ordered list(tuple).
     *
     * @return non-null list
     */
    default List<Object> asTuple() {
        return Arrays.asList(asArray());
    }

    /**
     * Gets value as UUID.
     *
     * @return uuid, could be null
     */
    default UUID asUuid() {
        if (isNullOrEmpty()) {
            return null;
        }

        return ClickHouseValues.convertToUuid(asBigInteger());
    }

    /**
     * Resets to default value of corresponding data type.
     *
     * @return this object
     */
    ClickHouseValue resetToDefault();

    /**
     * Resets value to null, or empty when null is not supported(e.g. Array, Tuple
     * and Map etc.).
     *
     * <p>
     * Keep in mind that String is value-based type, so this method will change its
     * value to null instead of an empty string.
     * 
     * @return this object
     */
    ClickHouseValue resetToNullOrEmpty();

    /**
     * Converts the value to escaped SQL expression. For example, number 123 will be
     * converted to {@code 123}, while string "12'3" will be converted to @{code
     * '12\'3'}.
     * 
     * @return escaped SQL expression
     */
    String toSqlExpression();

    /**
     * Updates value.
     *
     * @param value value to update
     * @return this object
     */
    default ClickHouseValue update(InputStream value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            update(new InputStreamReader(value, StandardCharsets.UTF_8));
        }

        return this;
    }

    /**
     * Updates value.
     *
     * @param value value to update
     * @return this object
     */
    default ClickHouseValue update(Reader value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            StringBuilder builder = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(value)) {
                int bufferSize = 1024;
                char[] buffer = new char[bufferSize];
                for (int num; (num = reader.read(buffer, 0, buffer.length)) > 0;) {
                    builder.append(buffer, 0, num);
                }
            } catch (IOException e) {
                throw new IllegalArgumentException("Failed to read", e);
            }
            update(builder.toString());
        }

        return this;
    }

    /**
     * Updates value.
     *
     * @param value value to update
     * @return this object
     */
    default ClickHouseValue update(boolean value) {
        return update(value ? (byte) 1 : (byte) 0);
    }

    /**
     * Updates value.
     *
     * @param value value to update
     * @return this object
     */
    default ClickHouseValue update(boolean[] value) {
        if (value == null || value.length == 0) {
            return resetToNullOrEmpty();
        } else if (value.length != 1) {
            throw new IllegalArgumentException(ClickHouseValues.ERROR_SINGLETON_ARRAY + Arrays.toString(value));
        }

        return update(value[0]);
    }

    /**
     * Updates value.
     *
     * @param value value to update
     * @return this object
     */
    default ClickHouseValue update(char value) {
        return update((int) value);
    }

    /**
     * Updates value.
     *
     * @param value value to update
     * @return this object
     */
    default ClickHouseValue update(char[] value) {
        if (value == null || value.length == 0) {
            return resetToNullOrEmpty();
        } else if (value.length != 1) {
            throw new IllegalArgumentException(ClickHouseValues.ERROR_SINGLETON_ARRAY + Arrays.toString(value));
        }

        return update(value[0]);
    }

    /**
     * Updates value.
     *
     * @param value value to update
     * @return this object
     */
    ClickHouseValue update(byte value);

    /**
     * Updates value.
     *
     * @param value value to update
     * @return this object
     */
    default ClickHouseValue update(byte[] value) {
        if (value == null || value.length == 0) {
            return resetToNullOrEmpty();
        } else if (value.length != 1) {
            throw new IllegalArgumentException(ClickHouseValues.ERROR_SINGLETON_ARRAY + Arrays.toString(value));
        }

        return update(value[0]);
    }

    /**
     * Updates value.
     *
     * @param value value to update
     * @return this object
     */
    ClickHouseValue update(short value);

    /**
     * Updates value.
     *
     * @param value value to update
     * @return this object
     */
    default ClickHouseValue update(short[] value) {
        if (value == null || value.length == 0) {
            return resetToNullOrEmpty();
        } else if (value.length != 1) {
            throw new IllegalArgumentException(ClickHouseValues.ERROR_SINGLETON_ARRAY + Arrays.toString(value));
        }

        return update(value[0]);
    }

    /**
     * Updates value.
     *
     * @param value value to update
     * @return this object
     */
    ClickHouseValue update(int value);

    /**
     * Updates value.
     *
     * @param value value to update
     * @return this object
     */
    default ClickHouseValue update(int[] value) {
        if (value == null || value.length == 0) {
            return resetToNullOrEmpty();
        } else if (value.length != 1) {
            throw new IllegalArgumentException(ClickHouseValues.ERROR_SINGLETON_ARRAY + Arrays.toString(value));
        }

        return update(value[0]);
    }

    /**
     * Updates value.
     *
     * @param value value to update
     * @return this object
     */
    ClickHouseValue update(long value);

    /**
     * Updates value.
     *
     * @param value value to update
     * @return this object
     */
    default ClickHouseValue update(long[] value) {
        if (value == null || value.length == 0) {
            return resetToNullOrEmpty();
        } else if (value.length != 1) {
            throw new IllegalArgumentException(ClickHouseValues.ERROR_SINGLETON_ARRAY + Arrays.toString(value));
        }

        return update(value[0]);
    }

    /**
     * Updates value.
     *
     * @param value value to update
     * @return this object
     */
    ClickHouseValue update(float value);

    /**
     * Updates value.
     *
     * @param value value to update
     * @return this object
     */
    default ClickHouseValue update(float[] value) {
        if (value == null || value.length == 0) {
            return resetToNullOrEmpty();
        } else if (value.length != 1) {
            throw new IllegalArgumentException(ClickHouseValues.ERROR_SINGLETON_ARRAY + Arrays.toString(value));
        }

        return update(value[0]);
    }

    /**
     * Updates value.
     *
     * @param value value to update
     * @return this object
     */
    ClickHouseValue update(double value);

    /**
     * Updates value.
     *
     * @param value value to update
     * @return this object
     */
    default ClickHouseValue update(double[] value) {
        if (value == null || value.length == 0) {
            return resetToNullOrEmpty();
        } else if (value.length != 1) {
            throw new IllegalArgumentException(ClickHouseValues.ERROR_SINGLETON_ARRAY + Arrays.toString(value));
        }

        return update(value[0]);
    }

    /**
     * Updates value.
     *
     * @param value value to update
     * @return this object
     */
    ClickHouseValue update(BigInteger value);

    /**
     * Updates value.
     *
     * @param value value to update
     * @return this object
     */
    ClickHouseValue update(BigDecimal value);

    /**
     * Updates value.
     *
     * @param value value to update
     * @return this object
     */
    default ClickHouseValue update(Enum<?> value) {
        return value == null ? resetToNullOrEmpty() : update(value.ordinal());
    }

    /**
     * Updates value.
     *
     * @param value value to update
     * @return this object
     */
    default ClickHouseValue update(Inet4Address value) {
        return value == null ? resetToNullOrEmpty() : update(new BigInteger(1, value.getAddress()));
    }

    /**
     * Updates value.
     *
     * @param value value to update
     * @return this object
     */
    default ClickHouseValue update(Inet6Address value) {
        return value == null ? resetToNullOrEmpty() : update(new BigInteger(1, value.getAddress()));
    }

    /**
     * Updates value.
     *
     * @param value value to update
     * @return this object
     */
    default ClickHouseValue update(LocalDate value) {
        return value == null ? resetToNullOrEmpty() : update(value.toEpochDay());
    }

    /**
     * Updates value.
     *
     * @param value value to update
     * @return this object
     */
    default ClickHouseValue update(LocalTime value) {
        return value == null ? resetToNullOrEmpty() : update(value.toSecondOfDay());
    }

    /**
     * Updates value.
     *
     * @param value value to update
     * @return this object
     */
    default ClickHouseValue update(LocalDateTime value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        long seconds = value.toEpochSecond(ZoneOffset.UTC);
        int nanos = value.getNano();
        return nanos > 0
                ? update(BigDecimal.valueOf(seconds).add(BigDecimal.valueOf(nanos, 9).divide(ClickHouseValues.NANOS)))
                : update(seconds);
    }

    /**
     * Updates value.
     *
     * @param value value to update
     * @return this object
     */
    default ClickHouseValue update(Instant value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        long seconds = value.getEpochSecond();
        int nanos = value.getNano();
        return nanos > 0
                ? update(BigDecimal.valueOf(seconds).add(BigDecimal.valueOf(nanos, 9).divide(ClickHouseValues.NANOS)))
                : update(seconds);
    }

    /**
     * Updates value.
     *
     * @param value value to update
     * @return this object
     */
    default ClickHouseValue update(OffsetDateTime value) {
        return update(value != null ? value.toLocalDateTime() : null);
    }

    /**
     * Updates value.
     *
     * @param value value to update
     * @return this object
     */
    default ClickHouseValue update(ZonedDateTime value) {
        return update(value != null ? value.toLocalDateTime() : null);
    }

    /**
     * Updates value.
     *
     * @param value value to update
     * @return this object
     */
    default ClickHouseValue update(Collection<?> value) {
        int size = value == null ? 0 : value.size();
        if (size == 0) {
            return resetToNullOrEmpty();
        } else if (size != 1) {
            throw new IllegalArgumentException(ClickHouseValues.ERROR_SINGLETON_COLLECTION + value);
        }

        return update(value.iterator().next());
    }

    /**
     * Updates value.
     *
     * @param value value to update
     * @return this object
     */
    default ClickHouseValue update(Enumeration<?> value) {
        if (value == null || !value.hasMoreElements()) {
            return resetToNullOrEmpty();
        }

        Object v = value.nextElement();
        if (value.hasMoreElements()) {
            throw new IllegalArgumentException(ClickHouseValues.ERROR_SINGLETON_COLLECTION + value);
        }

        return update(v);
    }

    /**
     * Updates value.
     *
     * @param value value to update
     * @return this object
     */
    default ClickHouseValue update(Map<?, ?> value) {
        int size = value == null ? 0 : value.size();
        if (size == 0) {
            return resetToNullOrEmpty();
        } else if (size != 1) {
            throw new IllegalArgumentException(ClickHouseValues.ERROR_SINGLETON_MAP + value);
        }

        return update(value.values().iterator().next());
    }

    /**
     * Updates value.
     *
     * @param value value to update
     * @return this object
     */
    ClickHouseValue update(String value);

    /**
     * Updates value.
     *
     * @param value value to update
     * @return this object
     */
    default ClickHouseValue update(UUID value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        BigInteger high = BigInteger.valueOf(value.getMostSignificantBits());
        BigInteger low = BigInteger.valueOf(value.getLeastSignificantBits());

        if (high.signum() < 0) {
            high = high.add(ClickHouseValues.BIGINT_HL_BOUNDARY);
        }
        if (low.signum() < 0) {
            low = low.add(ClickHouseValues.BIGINT_HL_BOUNDARY);
        }

        return update(low.add(high.multiply(ClickHouseValues.BIGINT_HL_BOUNDARY)));
    }

    /**
     * Updates value.
     *
     * @param value value to update
     * @return this object
     */
    ClickHouseValue update(ClickHouseValue value);

    /**
     * Updates value.
     *
     * @param value value to update
     * @return this object
     */
    default ClickHouseValue update(Object[] value) {
        if (value == null || value.length == 0) {
            return resetToNullOrEmpty();
        } else if (value.length != 1) {
            throw new IllegalArgumentException(ClickHouseValues.ERROR_SINGLETON_ARRAY + Arrays.toString(value));
        }

        return update(value[0]);
    }

    /**
     * Updates value when the type is not supported. This method will be called at
     * the end of {@link #update(Object)} after trying all known classes. By
     * default, it's same as {@code update(String.valueOf(value))}.
     *
     * <p>
     * Please avoid to call {@link #update(Object)} here as it will create endless
     * loop.
     * 
     * @param value value to update
     * @return this object
     */
    default ClickHouseValue updateUnknown(Object value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        return update(String.valueOf(value));
    }

    /**
     * Updates value. This method tries to identify type of {@code value} and then
     * use corresponding update method to proceed. Unknown value will be passed to
     * {@link #updateUnknown(Object)}.
     *
     * @param value value to update, could be null
     * @return this object
     */
    @SuppressWarnings("squid:S3776")
    default ClickHouseValue update(Object value) {
        if (value == null) {
            return resetToNullOrEmpty();
        } else if (value instanceof Boolean) {
            return update((boolean) value);
        } else if (value instanceof boolean[]) {
            return update((boolean[]) value);
        } else if (value instanceof Character) {
            return update((char) value);
        } else if (value instanceof char[]) {
            return update((char[]) value);
        } else if (value instanceof Byte) {
            return update((byte) value);
        } else if (value instanceof byte[]) {
            return update((byte[]) value);
        } else if (value instanceof Short) {
            return update((short) value);
        } else if (value instanceof short[]) {
            return update((short[]) value);
        } else if (value instanceof Integer) {
            return update((int) value);
        } else if (value instanceof int[]) {
            return update((int[]) value);
        } else if (value instanceof Long) {
            return update((long) value);
        } else if (value instanceof long[]) {
            return update((long[]) value);
        } else if (value instanceof Float) {
            return update((float) value);
        } else if (value instanceof float[]) {
            return update((float[]) value);
        } else if (value instanceof Double) {
            return update((double) value);
        } else if (value instanceof double[]) {
            return update((double[]) value);
        } else if (value instanceof BigDecimal) {
            return update((BigDecimal) value);
        } else if (value instanceof BigInteger) {
            return update((BigInteger) value);
        } else if (value instanceof Enum) {
            return update((Enum<?>) value);
        } else if (value instanceof Inet4Address) {
            return update((Inet4Address) value);
        } else if (value instanceof Inet6Address) {
            return update((Inet6Address) value);
        } else if (value instanceof LocalDate) {
            return update((LocalDate) value);
        } else if (value instanceof LocalTime) {
            return update((LocalTime) value);
        } else if (value instanceof LocalDateTime) {
            return update((LocalDateTime) value);
        } else if (value instanceof Instant) {
            return update((Instant) value);
        } else if (value instanceof OffsetDateTime) {
            return update((OffsetDateTime) value);
        } else if (value instanceof ZonedDateTime) {
            return update((ZonedDateTime) value);
        } else if (value instanceof Collection) {
            return update((Collection<?>) value);
        } else if (value instanceof Enumeration) {
            return update((Enumeration<?>) value);
        } else if (value instanceof Map) {
            return update((Map<?, ?>) value);
        } else if (value instanceof Object[]) {
            return update((Object[]) value);
        } else if (value instanceof UUID) {
            return update((UUID) value);
        } else if (value instanceof String) {
            return update((String) value);
        } else if (value instanceof InputStream) {
            return update((InputStream) value);
        } else if (value instanceof Reader) {
            return update((Reader) value);
        } else if (value instanceof ClickHouseValue) {
            return update((ClickHouseValue) value);
        } else {
            return updateUnknown(value);
        }
    }
}

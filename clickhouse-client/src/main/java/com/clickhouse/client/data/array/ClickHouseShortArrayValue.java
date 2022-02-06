package com.clickhouse.client.data.array;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Map.Entry;

import com.clickhouse.client.ClickHouseChecker;
import com.clickhouse.client.ClickHouseUtils;
import com.clickhouse.client.ClickHouseValue;
import com.clickhouse.client.ClickHouseValues;
import com.clickhouse.client.data.ClickHouseObjectValue;

/**
 * Wrapper of {@code short[]}.
 */
public class ClickHouseShortArrayValue extends ClickHouseObjectValue<short[]> {
    private static final String TYPE_NAME = "short[]";

    /**
     * Creates an empty array.
     *
     * @return empty array
     */

    public static ClickHouseShortArrayValue ofEmpty() {
        return of(ClickHouseValues.EMPTY_SHORT_ARRAY);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ClickHouseShortArrayValue of(short[] value) {
        return of(null, value);
    }

    /**
     * Update value of the given object or create a new instance if {@code ref} is
     * null.
     *
     * @param ref   object to update, could be null
     * @param value value
     * @return same object as {@code ref} or a new instance if it's null
     */

    public static ClickHouseShortArrayValue of(ClickHouseValue ref, short[] value) {
        return ref instanceof ClickHouseShortArrayValue ? ((ClickHouseShortArrayValue) ref).set(value)
                : new ClickHouseShortArrayValue(value);
    }

    protected ClickHouseShortArrayValue(short[] value) {
        super(value);
    }

    @Override
    protected ClickHouseShortArrayValue set(short[] value) {
        super.set(ClickHouseChecker.nonNull(value, ClickHouseValues.TYPE_ARRAY));
        return this;
    }

    @Override
    public Object[] asArray() {
        short[] v = getValue();
        int len = v.length;
        Short[] array = new Short[len];
        for (int i = 0; i < len; i++) {
            array[i] = Short.valueOf(v[i]);
        }
        return array;
    }

    @Override
    public <E> E[] asArray(Class<E> clazz) {
        short[] v = getValue();
        int len = v.length;
        E[] array = ClickHouseValues.createObjectArray(clazz, len, 1);
        for (int i = 0; i < len; i++) {
            array[i] = clazz.cast(v[i]);
        }
        return array;
    }

    @Override
    public <K, V> Map<K, V> asMap(Class<K> keyClass, Class<V> valueClass) {
        if (keyClass == null || valueClass == null) {
            throw new IllegalArgumentException("Non-null key and value classes are required");
        }
        short[] v = getValue();
        Map<K, V> map = new LinkedHashMap<>();
        for (int i = 0; i < v.length; i++) {
            map.put(keyClass.cast(i + 1), valueClass.cast(v[i]));
        }
        // why not use Collections.unmodifiableMap(map) here?
        return map;
    }

    @Override
    public String asString(int length, Charset charset) {
        String str = Arrays.toString(getValue());
        if (length > 0) {
            ClickHouseChecker.notWithDifferentLength(str.getBytes(charset == null ? StandardCharsets.UTF_8 : charset),
                    length);
        }

        return str;
    }

    @Override
    public ClickHouseShortArrayValue copy(boolean deep) {
        if (!deep) {
            return new ClickHouseShortArrayValue(getValue());
        }

        short[] value = getValue();
        return new ClickHouseShortArrayValue(Arrays.copyOf(value, value.length));
    }

    @Override
    public boolean isNullOrEmpty() {
        return getValue().length == 0;
    }

    @Override

    public ClickHouseShortArrayValue resetToNullOrEmpty() {
        set(ClickHouseValues.EMPTY_SHORT_ARRAY);
        return this;
    }

    @Override
    public String toSqlExpression() {
        short[] value = getValue();
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return ClickHouseValues.EMPTY_ARRAY_EXPR;
        }

        StringBuilder builder = new StringBuilder().append('[');
        for (int i = 0; i < len; i++) {
            builder.append(value[i]).append(',');
        }
        builder.setLength(builder.length() - 1);
        return builder.append(']').toString();
    }

    @Override
    public ClickHouseShortArrayValue update(boolean[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        short[] v = new short[len];
        for (int i = 0; i < len; i++) {
            v[i] = value[i] ? (short) 1 : (short) 0;
        }
        return set(v);
    }

    @Override
    public ClickHouseShortArrayValue update(char[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        short[] v = new short[len];
        for (int i = 0; i < len; i++) {
            v[i] = (short) value[i];
        }
        return set(v);
    }

    @Override
    public ClickHouseShortArrayValue update(byte value) {
        return set(new short[] { value });
    }

    @Override
    public ClickHouseShortArrayValue update(byte[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        short[] v = new short[len];
        for (int i = 0; i < len; i++) {
            v[i] = value[i];
        }
        return set(v);
    }

    @Override
    public ClickHouseShortArrayValue update(short value) {
        return set(new short[] { value });
    }

    @Override
    public ClickHouseShortArrayValue update(short[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        return set(Arrays.copyOf(value, len));
    }

    @Override
    public ClickHouseShortArrayValue update(int value) {
        return set(new short[] { (short) value });
    }

    @Override
    public ClickHouseShortArrayValue update(int[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        short[] v = new short[len];
        for (int i = 0; i < len; i++) {
            v[i] = (short) value[i];
        }
        return set(v);
    }

    @Override
    public ClickHouseShortArrayValue update(long value) {
        return set(new short[] { (short) value });
    }

    @Override
    public ClickHouseShortArrayValue update(long[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        short[] v = new short[len];
        for (int i = 0; i < len; i++) {
            v[i] = (short) value[i];
        }
        return set(v);
    }

    @Override
    public ClickHouseShortArrayValue update(float value) {
        return set(new short[] { (short) value });
    }

    @Override
    public ClickHouseShortArrayValue update(float[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        short[] v = new short[len];
        for (int i = 0; i < len; i++) {
            v[i] = (short) value[i];
        }
        return set(v);
    }

    @Override
    public ClickHouseShortArrayValue update(double value) {
        return set(new short[] { (short) value });
    }

    @Override
    public ClickHouseShortArrayValue update(double[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        short[] v = new short[len];
        for (int i = 0; i < len; i++) {
            v[i] = (short) value[i];
        }
        return set(v);
    }

    @Override
    public ClickHouseShortArrayValue update(BigInteger value) {
        return set(value == null ? ClickHouseValues.EMPTY_SHORT_ARRAY : new short[] { value.shortValue() });
    }

    @Override
    public ClickHouseShortArrayValue update(BigDecimal value) {
        return set(value == null ? ClickHouseValues.EMPTY_SHORT_ARRAY : new short[] { value.shortValue() });
    }

    @Override
    public ClickHouseShortArrayValue update(Enum<?> value) {
        return set(value == null ? ClickHouseValues.EMPTY_SHORT_ARRAY : new short[] { (short) value.ordinal() });
    }

    @Override
    public ClickHouseShortArrayValue update(Inet4Address value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        throw newUnsupportedException(ClickHouseValues.TYPE_IPV4, TYPE_NAME);
    }

    @Override
    public ClickHouseShortArrayValue update(Inet6Address value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        throw newUnsupportedException(ClickHouseValues.TYPE_IPV6, TYPE_NAME);
    }

    @Override
    public ClickHouseShortArrayValue update(LocalDate value) {
        return set(value == null ? ClickHouseValues.EMPTY_SHORT_ARRAY : new short[] { (short) value.toEpochDay() });
    }

    @Override
    public ClickHouseShortArrayValue update(LocalTime value) {
        return set(value == null ? ClickHouseValues.EMPTY_SHORT_ARRAY : new short[] { (short) value.toSecondOfDay() });
    }

    @Override
    public ClickHouseShortArrayValue update(LocalDateTime value) {
        return set(value == null ? ClickHouseValues.EMPTY_SHORT_ARRAY
                : new short[] { (short) value.toEpochSecond(ZoneOffset.UTC) });
    }

    @Override
    public ClickHouseShortArrayValue update(Collection<?> value) {
        int size = value == null ? 0 : value.size();
        if (size == 0) {
            return resetToNullOrEmpty();
        }

        short[] v = new short[size];
        int index = 0;
        for (Object o : value) {
            v[index++] = o == null ? 0 : ((Number) o).shortValue();
        }
        return set(v);
    }

    @Override
    public ClickHouseShortArrayValue update(Enumeration<?> value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        List<Number> v = new LinkedList<>();
        while (value.hasMoreElements()) {
            v.add((Number) value.nextElement());
        }

        short[] values = new short[v.size()];
        int index = 0;
        for (Number n : v) {
            values[index++] = n == null ? 0 : n.shortValue();
        }
        return set(values);
    }

    @Override
    public ClickHouseShortArrayValue update(Map<?, ?> value) {
        int size = value == null ? 0 : value.size();
        if (size == 0) {
            return resetToNullOrEmpty();
        }

        short[] v = new short[size];
        int index = 0;
        for (Entry<?, ?> e : value.entrySet()) {
            Object o = e.getValue();
            v[index++] = o == null ? 0 : ((Number) e.getValue()).shortValue();
        }
        return set(v);
    }

    @Override
    public ClickHouseShortArrayValue update(String value) {
        if (ClickHouseChecker.isNullOrBlank(value)) {
            set(ClickHouseValues.EMPTY_SHORT_ARRAY);
        } else {
            List<String> list = ClickHouseUtils.readValueArray(value, 0, value.length());
            if (list.isEmpty()) {
                set(ClickHouseValues.EMPTY_SHORT_ARRAY);
            } else {
                short[] arr = new short[list.size()];
                int index = 0;
                for (String v : list) {
                    arr[index++] = Short.parseShort(v);
                }
                set(arr);
            }
        }
        return this;
    }

    @Override
    public ClickHouseShortArrayValue update(UUID value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        throw newUnsupportedException(ClickHouseValues.TYPE_UUID, TYPE_NAME);
    }

    @Override
    public ClickHouseShortArrayValue update(ClickHouseValue value) {
        if (value == null || value.isNullOrEmpty()) {
            return resetToNullOrEmpty();
        } else if (value instanceof ClickHouseShortArrayValue) {
            set(((ClickHouseShortArrayValue) value).getValue());
        } else {
            update(value.asArray());
        }
        return this;
    }

    @Override
    public ClickHouseShortArrayValue update(Object[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        } else {
            short[] values = new short[len];
            for (int i = 0; i < len; i++) {
                Object o = value[i];
                values[i] = o == null ? 0 : ((Number) o).shortValue();
            }
            set(values);
        }

        return this;
    }

    @Override
    public ClickHouseValue updateUnknown(Object value) {
        if (value == null) {
            return resetToNullOrEmpty();
        } else if (value instanceof Number) {
            return set(new short[] { ((Number) value).shortValue() });
        } else {
            throw newUnsupportedException(value.getClass().getName(), TYPE_NAME);
        }
    }

    @Override
    public ClickHouseShortArrayValue update(Object value) {
        if (value instanceof short[]) {
            set((short[]) value);
        } else {
            super.update(value);
        }
        return this;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { // too bad this is a mutable class :<
            return true;
        } else if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        return Arrays.equals(getValue(), ((ClickHouseShortArrayValue) obj).getValue());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(getValue());
    }
}

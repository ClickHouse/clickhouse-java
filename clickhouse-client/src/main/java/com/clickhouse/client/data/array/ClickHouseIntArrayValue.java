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
 * Wrapper of {@code int[]}.
 */
public class ClickHouseIntArrayValue extends ClickHouseObjectValue<int[]> {
    private final static String TYPE_NAME = "int[]";

    /**
     * Creates an empty array.
     *
     * @return empty array
     */

    public static ClickHouseIntArrayValue ofEmpty() {
        return of(ClickHouseValues.EMPTY_INT_ARRAY);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ClickHouseIntArrayValue of(int[] value) {
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

    public static ClickHouseIntArrayValue of(ClickHouseValue ref, int[] value) {
        return ref instanceof ClickHouseIntArrayValue ? ((ClickHouseIntArrayValue) ref).set(value)
                : new ClickHouseIntArrayValue(value);
    }

    protected ClickHouseIntArrayValue(int[] value) {
        super(value);
    }

    @Override
    protected ClickHouseIntArrayValue set(int[] value) {
        super.set(ClickHouseChecker.nonNull(value, ClickHouseValues.TYPE_ARRAY));
        return this;
    }

    @Override
    public Object[] asArray() {
        int[] v = getValue();
        int len = v.length;
        Integer[] array = new Integer[len];
        for (int i = 0; i < len; i++) {
            array[i] = Integer.valueOf(v[i]);
        }
        return array;
    }

    @Override
    public <E> E[] asArray(Class<E> clazz) {
        int[] v = getValue();
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
        int[] v = getValue();
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
    public ClickHouseIntArrayValue copy(boolean deep) {
        if (!deep) {
            return new ClickHouseIntArrayValue(getValue());
        }

        int[] value = getValue();
        return new ClickHouseIntArrayValue(Arrays.copyOf(value, value.length));
    }

    @Override
    public boolean isNullOrEmpty() {
        return getValue().length == 0;
    }

    @Override
    public ClickHouseIntArrayValue resetToDefault() {
        set(ClickHouseValues.EMPTY_INT_ARRAY);
        return this;
    }

    @Override
    public ClickHouseIntArrayValue resetToNullOrEmpty() {
        return resetToDefault();
    }

    @Override
    public String toSqlExpression() {
        int[] value = getValue();
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
    public ClickHouseIntArrayValue update(boolean[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        int[] v = new int[len];
        for (int i = 0; i < len; i++) {
            v[i] = value[i] ? 1 : 0;
        }
        return set(v);
    }

    @Override
    public ClickHouseIntArrayValue update(char[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        int[] v = new int[len];
        for (int i = 0; i < len; i++) {
            v[i] = value[i];
        }
        return set(v);
    }

    @Override
    public ClickHouseIntArrayValue update(byte value) {
        return set(new int[] { value });
    }

    @Override
    public ClickHouseIntArrayValue update(byte[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        int[] v = new int[len];
        for (int i = 0; i < len; i++) {
            v[i] = value[i];
        }
        return set(v);
    }

    @Override
    public ClickHouseIntArrayValue update(short value) {
        return set(new int[] { value });
    }

    @Override
    public ClickHouseIntArrayValue update(short[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        int[] v = new int[len];
        for (int i = 0; i < len; i++) {
            v[i] = value[i];
        }
        return set(v);
    }

    @Override
    public ClickHouseIntArrayValue update(int value) {
        return set(new int[] { value });
    }

    @Override
    public ClickHouseIntArrayValue update(int[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        return set(Arrays.copyOf(value, len));
    }

    @Override
    public ClickHouseIntArrayValue update(long value) {
        return set(new int[] { (int) value });
    }

    @Override
    public ClickHouseIntArrayValue update(long[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        int[] v = new int[len];
        for (int i = 0; i < len; i++) {
            v[i] = (int) value[i];
        }
        return set(v);
    }

    @Override
    public ClickHouseIntArrayValue update(float value) {
        return set(new int[] { (int) value });
    }

    @Override
    public ClickHouseIntArrayValue update(float[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        int[] v = new int[len];
        for (int i = 0; i < len; i++) {
            v[i] = (int) value[i];
        }
        return set(v);
    }

    @Override
    public ClickHouseIntArrayValue update(double value) {
        return set(new int[] { (int) value });
    }

    @Override
    public ClickHouseIntArrayValue update(double[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        int[] v = new int[len];
        for (int i = 0; i < len; i++) {
            v[i] = (int) value[i];
        }
        return set(v);
    }

    @Override
    public ClickHouseIntArrayValue update(BigInteger value) {
        return set(value == null ? ClickHouseValues.EMPTY_INT_ARRAY : new int[] { value.intValue() });
    }

    @Override
    public ClickHouseIntArrayValue update(BigDecimal value) {
        return set(value == null ? ClickHouseValues.EMPTY_INT_ARRAY : new int[] { value.intValue() });
    }

    @Override
    public ClickHouseIntArrayValue update(Enum<?> value) {
        return set(value == null ? ClickHouseValues.EMPTY_INT_ARRAY : new int[] { value.ordinal() });
    }

    @Override
    public ClickHouseIntArrayValue update(Inet4Address value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        throw newUnsupportedException(ClickHouseValues.TYPE_IPV4, TYPE_NAME);
    }

    @Override
    public ClickHouseIntArrayValue update(Inet6Address value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        throw newUnsupportedException(ClickHouseValues.TYPE_IPV6, TYPE_NAME);
    }

    @Override
    public ClickHouseIntArrayValue update(LocalDate value) {
        return set(value == null ? ClickHouseValues.EMPTY_INT_ARRAY : new int[] { (int) value.toEpochDay() });
    }

    @Override
    public ClickHouseIntArrayValue update(LocalTime value) {
        return set(value == null ? ClickHouseValues.EMPTY_INT_ARRAY : new int[] { value.toSecondOfDay() });
    }

    @Override
    public ClickHouseIntArrayValue update(LocalDateTime value) {
        return set(value == null ? ClickHouseValues.EMPTY_INT_ARRAY
                : new int[] { (int) value.toEpochSecond(ZoneOffset.UTC) });
    }

    @Override
    public ClickHouseIntArrayValue update(Collection<?> value) {
        int size = value == null ? 0 : value.size();
        if (size == 0) {
            return resetToNullOrEmpty();
        }

        int[] v = new int[size];
        int index = 0;
        for (Object o : value) {
            v[index++] = o == null ? 0 : ((Number) o).intValue();
        }
        return set(v);
    }

    @Override
    public ClickHouseIntArrayValue update(Enumeration<?> value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        List<Number> v = new LinkedList<>();
        while (value.hasMoreElements()) {
            v.add((Number) value.nextElement());
        }

        int[] values = new int[v.size()];
        int index = 0;
        for (Number n : v) {
            values[index++] = n == null ? 0 : n.intValue();
        }
        return set(values);
    }

    @Override
    public ClickHouseIntArrayValue update(Map<?, ?> value) {
        int size = value == null ? 0 : value.size();
        if (size == 0) {
            return resetToNullOrEmpty();
        }

        int[] v = new int[size];
        int index = 0;
        for (Entry<?, ?> e : value.entrySet()) {
            Object o = e.getValue();
            v[index++] = o == null ? 0 : ((Number) e.getValue()).intValue();
        }
        return set(v);
    }

    @Override
    public ClickHouseIntArrayValue update(String value) {
        if (ClickHouseChecker.isNullOrBlank(value)) {
            set(ClickHouseValues.EMPTY_INT_ARRAY);
        } else {
            List<String> list = ClickHouseUtils.readValueArray(value, 0, value.length());
            if (list.isEmpty()) {
                set(ClickHouseValues.EMPTY_INT_ARRAY);
            } else {
                int[] arr = new int[list.size()];
                int index = 0;
                for (String v : list) {
                    arr[index++] = v == null ? 0 : Integer.parseInt(v);
                }
                set(arr);
            }
        }
        return this;
    }

    @Override
    public ClickHouseIntArrayValue update(UUID value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        throw newUnsupportedException(ClickHouseValues.TYPE_UUID, TYPE_NAME);
    }

    @Override
    public ClickHouseIntArrayValue update(ClickHouseValue value) {
        if (value == null || value.isNullOrEmpty()) {
            return resetToNullOrEmpty();
        } else if (value instanceof ClickHouseIntArrayValue) {
            set(((ClickHouseIntArrayValue) value).getValue());
        } else {
            update(value.asArray());
        }
        return this;
    }

    @Override
    public ClickHouseIntArrayValue update(Object[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        } else {
            int[] values = new int[len];
            for (int i = 0; i < len; i++) {
                Object o = value[i];
                values[i] = o == null ? 0 : ((Number) o).intValue();
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
            return set(new int[] { ((Number) value).intValue() });
        } else {
            throw newUnsupportedException(value.getClass().getName(), TYPE_NAME);
        }
    }

    @Override
    public ClickHouseIntArrayValue update(Object value) {
        if (value instanceof int[]) {
            set((int[]) value);
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

        return Arrays.equals(getValue(), ((ClickHouseIntArrayValue) obj).getValue());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(getValue());
    }
}

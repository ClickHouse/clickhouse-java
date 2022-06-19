package com.clickhouse.client.data;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
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
import com.clickhouse.client.ClickHouseValue;
import com.clickhouse.client.ClickHouseValues;

/**
 * Wrapper class of Array.
 */
public class ClickHouseArrayValue<T> extends ClickHouseObjectValue<T[]> {
    /**
     * Creates an empty array.
     *
     * @param <T> type of the array
     * @return empty array
     */
    @SuppressWarnings("unchecked")
    public static <T> ClickHouseArrayValue<T> ofEmpty() {
        return of((T[]) ClickHouseValues.EMPTY_OBJECT_ARRAY);
    }

    /**
     * Wrap the given value.
     *
     * @param <T>   type of element
     * @param value value
     * @return object representing the value
     */
    public static <T> ClickHouseArrayValue<T> of(T[] value) {
        return of(null, value);
    }

    /**
     * Update value of the given object or create a new instance if {@code ref} is
     * null.
     *
     * @param <T>   type of element
     * @param ref   object to update, could be null
     * @param value value
     * @return same object as {@code ref} or a new instance if it's null
     */
    @SuppressWarnings("unchecked")
    public static <T> ClickHouseArrayValue<T> of(ClickHouseValue ref, T[] value) {
        return ref instanceof ClickHouseArrayValue
                ? (ClickHouseArrayValue<T>) ((ClickHouseArrayValue<T>) ref).set(value)
                : new ClickHouseArrayValue<>(value);
    }

    protected ClickHouseArrayValue(T[] value) {
        super(value);
    }

    @Override
    protected ClickHouseArrayValue<T> set(T[] value) {
        super.set(ClickHouseChecker.nonNull(value, ClickHouseValues.TYPE_ARRAY));
        return this;
    }

    @Override
    public Object[] asArray() {
        return getValue();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E> E[] asArray(Class<E> clazz) {
        T[] v = getValue();
        E[] array = (E[]) Array.newInstance(ClickHouseChecker.nonNull(clazz, ClickHouseValues.TYPE_CLASS), v.length);
        int index = 0;
        for (T o : v) {
            array[index++] = clazz.cast(o);
        }
        return array;
    }

    @Override
    public <K, V> Map<K, V> asMap(Class<K> keyClass, Class<V> valueClass) {
        if (keyClass == null || valueClass == null) {
            throw new IllegalArgumentException("Non-null key and value classes are required");
        }
        Map<K, V> map = new LinkedHashMap<>();
        int index = 1;
        for (Object o : getValue()) {
            map.put(keyClass.cast(index++), valueClass.cast(o));
        }
        // why not use Collections.unmodifiableMap(map) here?
        return map;
    }

    @Override
    public String asString(int length, Charset charset) {
        String str = Arrays.deepToString(getValue());
        if (length > 0) {
            ClickHouseChecker.notWithDifferentLength(str.getBytes(charset == null ? StandardCharsets.UTF_8 : charset),
                    length);
        }

        return str;
    }

    @Override
    public ClickHouseArrayValue<T> copy(boolean deep) {
        if (!deep) {
            return new ClickHouseArrayValue<>(getValue());
        }

        T[] value = getValue();
        T[] newValue = Arrays.copyOf(value, value.length); // try harder
        return new ClickHouseArrayValue<>(newValue);
    }

    @Override
    public boolean isNullOrEmpty() {
        return getValue().length == 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ClickHouseArrayValue<T> resetToDefault() {
        set((T[]) ClickHouseValues.EMPTY_OBJECT_ARRAY);
        return this;
    }

    @Override
    public ClickHouseArrayValue<T> resetToNullOrEmpty() {
        return resetToDefault();
    }

    @Override
    public String toSqlExpression() {
        T[] value = getValue();
        if (value == null || value.length == 0) {
            return ClickHouseValues.EMPTY_ARRAY_EXPR;
        }

        StringBuilder builder = new StringBuilder().append('[');
        for (T v : value) {
            builder.append(ClickHouseValues.convertToSqlExpression(v)).append(',');
        }
        if (builder.length() > 1) {
            builder.setLength(builder.length() - 1);
        }
        return builder.append(']').toString();
    }

    @Override
    @SuppressWarnings("unchecked")
    public ClickHouseArrayValue<T> update(boolean[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        Object[] v = new Object[len];
        int index = 0;
        for (boolean b : value) {
            v[index++] = b ? (byte) 1 : (byte) 0;
        }
        set((T[]) v);
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ClickHouseArrayValue<T> update(char[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        Object[] v = new Object[len];
        int index = 0;
        for (char c : value) {
            v[index++] = (int) c;
        }
        set((T[]) v);
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ClickHouseArrayValue<T> update(byte value) {
        set((T[]) new Byte[] { value });
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ClickHouseArrayValue<T> update(byte[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        Byte[] v = new Byte[len];
        int index = 0;
        for (byte b : value) {
            v[index++] = b;
        }
        set((T[]) v);
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ClickHouseArrayValue<T> update(short value) {
        set((T[]) new Short[] { value });
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ClickHouseArrayValue<T> update(short[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        Short[] v = new Short[len];
        int index = 0;
        for (short s : value) {
            v[index++] = s;
        }
        set((T[]) v);
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ClickHouseArrayValue<T> update(int value) {
        set((T[]) new Integer[] { value });
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ClickHouseArrayValue<T> update(int[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        Integer[] v = new Integer[len];
        int index = 0;
        for (int i : value) {
            v[index++] = i;
        }
        set((T[]) v);
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ClickHouseArrayValue<T> update(long value) {
        set((T[]) new Long[] { value });
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ClickHouseArrayValue<T> update(long[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        Long[] v = new Long[len];
        int index = 0;
        for (long l : value) {
            v[index++] = l;
        }
        set((T[]) v);
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ClickHouseArrayValue<T> update(float value) {
        set((T[]) new Float[] { value });
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ClickHouseArrayValue<T> update(float[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        Float[] v = new Float[len];
        int index = 0;
        for (float f : value) {
            v[index++] = f;
        }
        set((T[]) v);
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ClickHouseArrayValue<T> update(double value) {
        set((T[]) new Double[] { value });
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ClickHouseArrayValue<T> update(double[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        Double[] v = new Double[len];
        int index = 0;
        for (double d : value) {
            v[index++] = d;
        }
        set((T[]) v);
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ClickHouseArrayValue<T> update(BigInteger value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        return set((T[]) new BigInteger[] { value });
    }

    @Override
    @SuppressWarnings("unchecked")
    public ClickHouseArrayValue<T> update(BigDecimal value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        return set((T[]) new BigDecimal[] { value });
    }

    @Override
    @SuppressWarnings("unchecked")
    public ClickHouseArrayValue<T> update(Enum<?> value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        return set((T[]) new Enum<?>[] { value });
    }

    @Override
    @SuppressWarnings("unchecked")
    public ClickHouseArrayValue<T> update(Inet4Address value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        return set((T[]) new Inet4Address[] { value });
    }

    @Override
    @SuppressWarnings("unchecked")
    public ClickHouseArrayValue<T> update(Inet6Address value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        return set((T[]) new Inet6Address[] { value });
    }

    @Override
    @SuppressWarnings("unchecked")
    public ClickHouseArrayValue<T> update(LocalDate value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        return set((T[]) new LocalDate[] { value });
    }

    @Override
    @SuppressWarnings("unchecked")
    public ClickHouseArrayValue<T> update(LocalTime value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        return set((T[]) new LocalTime[] { value });
    }

    @Override
    @SuppressWarnings("unchecked")
    public ClickHouseArrayValue<T> update(LocalDateTime value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        return set((T[]) new LocalDateTime[] { value });
    }

    @Override
    @SuppressWarnings("unchecked")
    public ClickHouseArrayValue<T> update(Collection<?> value) {
        int size = value == null ? 0 : value.size();
        if (size == 0) {
            return resetToNullOrEmpty();
        }

        Object[] v = new Object[size];
        int index = 0;
        for (Object o : value) {
            v[index++] = o;
        }
        set((T[]) v);
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ClickHouseArrayValue<T> update(Enumeration<?> value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        List<Object> v = new LinkedList<>();
        while (value.hasMoreElements()) {
            v.add(value.nextElement());
        }
        set((T[]) v.toArray(new Object[v.size()]));
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ClickHouseArrayValue<T> update(Map<?, ?> value) {
        int size = value == null ? 0 : value.size();
        if (size == 0) {
            return resetToNullOrEmpty();
        }

        Object[] v = new Object[size];
        int index = 0;
        for (Entry<?, ?> e : value.entrySet()) {
            v[index++] = e.getValue();
        }
        set((T[]) v);
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ClickHouseArrayValue<T> update(String value) {
        set((T[]) new String[] { value });
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ClickHouseArrayValue<T> update(UUID value) {
        set((T[]) new UUID[] { value });
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ClickHouseArrayValue<T> update(ClickHouseValue value) {
        if (value == null) {
            return resetToNullOrEmpty();
        } else if (value instanceof ClickHouseArrayValue) {
            set(((ClickHouseArrayValue<T>) value).getValue());
        } else {
            set(value.isNullOrEmpty() ? (T[]) ClickHouseValues.EMPTY_OBJECT_ARRAY : (T[]) value.asArray());
        }
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ClickHouseArrayValue<T> update(Object[] value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        set((T[]) value);
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ClickHouseValue updateUnknown(Object value) {
        Object[] v = (Object[]) Array.newInstance(value == null ? Object.class : value.getClass(), 1);
        v[0] = value;
        set((T[]) v);
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ClickHouseArrayValue<T> update(Object value) {
        if (value instanceof Object[]) {
            set((T[]) value);
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

        return Arrays.deepEquals(getValue(), ((ClickHouseArrayValue<?>) obj).getValue());
    }

    @Override
    public int hashCode() {
        return Arrays.deepHashCode(getValue());
    }
}

package com.clickhouse.data.value.array;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
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
import com.clickhouse.data.ClickHouseArraySequence;
import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseUtils;
import com.clickhouse.data.ClickHouseValue;
import com.clickhouse.data.ClickHouseValues;
import com.clickhouse.data.value.ClickHouseObjectValue;
import com.clickhouse.data.value.UnsignedLong;

/**
 * Wrapper of {@code long[]}.
 */
@Deprecated
public class ClickHouseLongArrayValue extends ClickHouseObjectValue<long[]> implements ClickHouseArraySequence {
    static final class UnsignedLongArrayValue extends ClickHouseLongArrayValue {
        @Override
        public Object[] asArray() {
            long[] v = getValue();
            int len = v.length;
            UnsignedLong[] array = new UnsignedLong[len];
            for (int i = 0; i < len; i++) {
                array[i] = UnsignedLong.valueOf(v[i]);
            }
            return array;
        }

        @Override
        public String asString() {
            long[] v = getValue();
            int len = v.length;
            if (len == 0) {
                return ClickHouseValues.EMPTY_ARRAY_EXPR;
            }
            StringBuilder builder = new StringBuilder().append('[').append(Long.toUnsignedString(v[0]));
            for (int i = 1; i < len; i++) {
                builder.append(',').append(Long.toUnsignedString(v[i]));
            }
            return builder.append(']').toString();
        }

        @Override
        public UnsignedLongArrayValue copy(boolean deep) {
            if (!deep) {
                return new UnsignedLongArrayValue(getValue());
            }

            long[] value = getValue();
            return new UnsignedLongArrayValue(Arrays.copyOf(value, value.length));
        }

        @Override
        public UnsignedLongArrayValue update(String value) {
            if (ClickHouseChecker.isNullOrBlank(value)) {
                set(ClickHouseValues.EMPTY_LONG_ARRAY);
            } else {
                List<String> list = ClickHouseUtils.readValueArray(value, 0, value.length());
                if (list.isEmpty()) {
                    set(ClickHouseValues.EMPTY_LONG_ARRAY);
                } else {
                    long[] arr = new long[list.size()];
                    int index = 0;
                    for (String v : list) {
                        arr[index++] = v == null ? 0L : Long.parseUnsignedLong(v);
                    }
                    set(arr);
                }
            }
            return this;
        }

        protected UnsignedLongArrayValue(long[] value) {
            super(value);
        }
    }

    private static final String TYPE_NAME = "long[]";

    /**
     * Creates a new instance representing empty {@code Int64} array.
     *
     * @return new instance representing an empty array
     */
    public static ClickHouseLongArrayValue ofEmpty() {
        return of(null, ClickHouseValues.EMPTY_LONG_ARRAY, false);
    }

    /**
     * Creates a new instance representing empty {@code UInt64} array.
     *
     * @return new instance representing an empty array
     */
    public static ClickHouseLongArrayValue ofUnsignedEmpty() {
        return of(null, ClickHouseValues.EMPTY_LONG_ARRAY, true);
    }

    /**
     * Wraps the given {@code Int64} array.
     *
     * @param value value
     * @return object representing the value
     */
    public static ClickHouseLongArrayValue of(long[] value) {
        return of(null, value, false);
    }

    /**
     * Wraps the given {@code UInt64} array.
     *
     * @param value value
     * @return object representing the value
     */
    public static ClickHouseLongArrayValue ofUnsigned(long[] value) {
        return of(null, value, true);
    }

    /**
     * Update value of the given object or create a new instance if {@code ref} is
     * null.
     *
     * @param ref      object to update, could be null
     * @param value    value
     * @param unsigned true if {@code value} is unsigned; false otherwise
     * @return same object as {@code ref} or a new instance if it's null
     */

    public static ClickHouseLongArrayValue of(ClickHouseValue ref, long[] value, boolean unsigned) {
        if (unsigned) {
            return ref instanceof UnsignedLongArrayValue ? ((UnsignedLongArrayValue) ref).set(value)
                    : new UnsignedLongArrayValue(value);
        }

        return ref instanceof ClickHouseLongArrayValue ? ((ClickHouseLongArrayValue) ref).set(value)
                : new ClickHouseLongArrayValue(value);
    }

    protected ClickHouseLongArrayValue(long[] value) {
        super(value);
    }

    @Override
    protected ClickHouseLongArrayValue set(long[] value) {
        super.set(ClickHouseChecker.nonNull(value, ClickHouseValues.TYPE_ARRAY));
        return this;
    }

    @Override
    public Object[] asArray() {
        long[] v = getValue();
        int len = v.length;
        Long[] array = new Long[len];
        for (int i = 0; i < len; i++) {
            array[i] = Long.valueOf(v[i]);
        }
        return array;
    }

    @Override
    public <E> E[] asArray(Class<E> clazz) {
        long[] v = getValue();
        int len = v.length;
        if (clazz == BigInteger.class) {
            BigInteger[] array = new BigInteger[len];
            for (int i = 0; i < len; i++) {
                long value = v[i];
                array[i] = BigInteger.valueOf(value);
                if (value < 0L) {
                    array[i] = array[i].and(UnsignedLong.MASK);
                }
            }
            return (E[]) array;
        } else {
            E[] array = ClickHouseValues.createObjectArray(clazz, len, 1);
            for (int i = 0; i < len; i++) {
                array[i] = clazz.cast(v[i]);
            }
            return array;
        }
    }

    @Override
    public <K, V> Map<K, V> asMap(Class<K> keyClass, Class<V> valueClass) {
        if (keyClass == null || valueClass == null) {
            throw new IllegalArgumentException("Non-null key and value classes are required");
        }
        long[] v = getValue();
        Map<K, V> map = new LinkedHashMap<>();
        for (int i = 0; i < v.length; i++) {
            map.put(keyClass.cast(i + 1), valueClass.cast(v[i]));
        }
        // why not use Collections.unmodifiableMap(map) here?
        return map;
    }

    @Override
    public String asString() {
        long[] value = getValue();
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return ClickHouseValues.EMPTY_ARRAY_EXPR;
        }

        StringBuilder builder = new StringBuilder().append('[').append(value[0]);
        for (int i = 1; i < len; i++) {
            builder.append(',').append(value[i]);
        }
        return builder.append(']').toString();
    }

    @Override
    public ClickHouseLongArrayValue copy(boolean deep) {
        if (!deep) {
            return new ClickHouseLongArrayValue(getValue());
        }

        long[] value = getValue();
        return new ClickHouseLongArrayValue(Arrays.copyOf(value, value.length));
    }

    @Override
    public boolean isNullable() {
        return false;
    }

    @Override
    public boolean isNullOrEmpty() {
        return getValue().length == 0;
    }

    @Override
    public ClickHouseLongArrayValue resetToDefault() {
        set(ClickHouseValues.EMPTY_LONG_ARRAY);
        return this;
    }

    @Override
    public ClickHouseLongArrayValue resetToNullOrEmpty() {
        return resetToDefault();
    }

    @Override
    public String toSqlExpression() {
        return asString();
    }

    @Override
    public ClickHouseLongArrayValue update(boolean[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        long[] v = new long[len];
        for (int i = 0; i < len; i++) {
            v[i] = value[i] ? 1L : 0L;
        }
        return set(v);
    }

    @Override
    public ClickHouseLongArrayValue update(char[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        long[] v = new long[len];
        for (int i = 0; i < len; i++) {
            v[i] = value[i];
        }
        return set(v);
    }

    @Override
    public ClickHouseLongArrayValue update(byte value) {
        return set(new long[] { value });
    }

    @Override
    public ClickHouseLongArrayValue update(byte[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        long[] v = new long[len];
        for (int i = 0; i < len; i++) {
            v[i] = 0xFFL & value[i];
        }
        return set(v);
    }

    @Override
    public ClickHouseLongArrayValue update(short value) {
        return set(new long[] { value });
    }

    @Override
    public ClickHouseLongArrayValue update(short[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        long[] v = new long[len];
        for (int i = 0; i < len; i++) {
            v[i] = 0xFFFFL & value[i];
        }
        return set(v);
    }

    @Override
    public ClickHouseLongArrayValue update(int value) {
        return set(new long[] { value });
    }

    @Override
    public ClickHouseLongArrayValue update(int[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        long[] v = new long[len];
        for (int i = 0; i < len; i++) {
            v[i] = 0xFFFFFFFFL & value[i];
        }
        return set(v);
    }

    @Override
    public ClickHouseLongArrayValue update(long value) {
        return set(new long[] { value });
    }

    @Override
    public ClickHouseLongArrayValue update(long[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        return set(value);
    }

    @Override
    public ClickHouseLongArrayValue update(float value) {
        return set(new long[] { (long) value });
    }

    @Override
    public ClickHouseLongArrayValue update(float[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        long[] v = new long[len];
        for (int i = 0; i < len; i++) {
            v[i] = (long) value[i];
        }
        return set(v);
    }

    @Override
    public ClickHouseLongArrayValue update(double value) {
        return set(new long[] { (long) value });
    }

    @Override
    public ClickHouseLongArrayValue update(double[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        long[] v = new long[len];
        for (int i = 0; i < len; i++) {
            v[i] = (long) value[i];
        }
        return set(v);
    }

    @Override
    public ClickHouseLongArrayValue update(BigInteger value) {
        return set(value == null ? ClickHouseValues.EMPTY_LONG_ARRAY : new long[] { value.longValue() });
    }

    @Override
    public ClickHouseLongArrayValue update(BigDecimal value) {
        return set(value == null ? ClickHouseValues.EMPTY_LONG_ARRAY : new long[] { value.longValue() });
    }

    @Override
    public ClickHouseLongArrayValue update(Enum<?> value) {
        return set(value == null ? ClickHouseValues.EMPTY_LONG_ARRAY : new long[] { value.ordinal() });
    }

    @Override
    public ClickHouseLongArrayValue update(Inet4Address value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        throw newUnsupportedException(ClickHouseValues.TYPE_IPV4, TYPE_NAME);
    }

    @Override
    public ClickHouseLongArrayValue update(Inet6Address value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        throw newUnsupportedException(ClickHouseValues.TYPE_IPV6, TYPE_NAME);
    }

    @Override
    public ClickHouseLongArrayValue update(LocalDate value) {
        return set(value == null ? ClickHouseValues.EMPTY_LONG_ARRAY : new long[] { value.toEpochDay() });
    }

    @Override
    public ClickHouseLongArrayValue update(LocalTime value) {
        return set(value == null ? ClickHouseValues.EMPTY_LONG_ARRAY : new long[] { value.toSecondOfDay() });
    }

    @Override
    public ClickHouseLongArrayValue update(LocalDateTime value) {
        return set(
                value == null ? ClickHouseValues.EMPTY_LONG_ARRAY : new long[] { value.toEpochSecond(ZoneOffset.UTC) });
    }

    @Override
    public ClickHouseLongArrayValue update(Collection<?> value) {
        int size = value == null ? 0 : value.size();
        if (size == 0) {
            return resetToNullOrEmpty();
        }

        long[] v = new long[size];
        int index = 0;
        for (Object o : value) {
            v[index++] = o == null ? 0 : ((Number) o).longValue();
        }
        return set(v);
    }

    @Override
    public ClickHouseLongArrayValue update(Enumeration<?> value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        List<Number> v = new LinkedList<>();
        while (value.hasMoreElements()) {
            v.add((Number) value.nextElement());
        }

        long[] values = new long[v.size()];
        int index = 0;
        for (Number n : v) {
            values[index++] = n == null ? 0 : n.longValue();
        }
        return set(values);
    }

    @Override
    public ClickHouseLongArrayValue update(Map<?, ?> value) {
        int size = value == null ? 0 : value.size();
        if (size == 0) {
            return resetToNullOrEmpty();
        }

        long[] v = new long[size];
        int index = 0;
        for (Entry<?, ?> e : value.entrySet()) {
            Object o = e.getValue();
            v[index++] = o == null ? 0 : ((Number) e.getValue()).longValue();
        }
        return set(v);
    }

    @Override
    public ClickHouseLongArrayValue update(String value) {
        if (ClickHouseChecker.isNullOrBlank(value)) {
            set(ClickHouseValues.EMPTY_LONG_ARRAY);
        } else {
            List<String> list = ClickHouseUtils.readValueArray(value, 0, value.length());
            if (list.isEmpty()) {
                set(ClickHouseValues.EMPTY_LONG_ARRAY);
            } else {
                long[] arr = new long[list.size()];
                int index = 0;
                for (String v : list) {
                    arr[index++] = v == null ? 0L : Long.parseLong(v);
                }
                set(arr);
            }
        }
        return this;
    }

    @Override
    public ClickHouseLongArrayValue update(UUID value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        throw newUnsupportedException(ClickHouseValues.TYPE_UUID, TYPE_NAME);
    }

    @Override
    public ClickHouseLongArrayValue update(ClickHouseValue value) {
        if (value == null || value.isNullOrEmpty()) {
            return resetToNullOrEmpty();
        } else if (value instanceof ClickHouseLongArrayValue) {
            set(((ClickHouseLongArrayValue) value).getValue());
        } else {
            update(value.asArray());
        }
        return this;
    }

    @Override
    public ClickHouseLongArrayValue update(Object[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        } else {
            long[] values = new long[len];
            for (int i = 0; i < len; i++) {
                Object o = value[i];
                values[i] = o == null ? 0 : ((Number) o).longValue();
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
            return set(new long[] { ((Number) value).longValue() });
        } else {
            throw newUnsupportedException(value.getClass().getName(), TYPE_NAME);
        }
    }

    @Override
    public ClickHouseLongArrayValue update(Object value) {
        if (value instanceof long[]) {
            set((long[]) value);
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

        return Arrays.equals(getValue(), ((ClickHouseLongArrayValue) obj).getValue());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(getValue());
    }

    @Override
    public ClickHouseArraySequence allocate(int length, Class<?> clazz, int level) {
        if (length < 1) {
            resetToDefault();
        } else {
            set(new long[length]);
        }
        return this;
    }

    @Override
    public int length() {
        return isNullOrEmpty() ? 0 : getValue().length;
    }

    @Override
    public <V extends ClickHouseValue> V getValue(int index, V value) {
        value.update(getValue()[index]);
        return value;
    }

    @Override
    public ClickHouseArraySequence setValue(int index, ClickHouseValue value) {
        getValue()[index] = value.asLong();
        return this;
    }
}

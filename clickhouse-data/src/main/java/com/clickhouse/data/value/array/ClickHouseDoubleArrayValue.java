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

/**
 * Wrapper of {@code double[]}.
 */
@Deprecated
public class ClickHouseDoubleArrayValue extends ClickHouseObjectValue<double[]> implements ClickHouseArraySequence {
    private static final String TYPE_NAME = "double[]";

    /**
     * Creates an empty array.
     *
     * @return empty array
     */

    public static ClickHouseDoubleArrayValue ofEmpty() {
        return of(ClickHouseValues.EMPTY_DOUBLE_ARRAY);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ClickHouseDoubleArrayValue of(double[] value) {
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

    public static ClickHouseDoubleArrayValue of(ClickHouseValue ref, double[] value) {
        return ref instanceof ClickHouseDoubleArrayValue ? ((ClickHouseDoubleArrayValue) ref).set(value)
                : new ClickHouseDoubleArrayValue(value);
    }

    protected ClickHouseDoubleArrayValue(double[] value) {
        super(value);
    }

    @Override
    protected ClickHouseDoubleArrayValue set(double[] value) {
        super.set(ClickHouseChecker.nonNull(value, ClickHouseValues.TYPE_ARRAY));
        return this;
    }

    @Override
    public Object[] asArray() {
        double[] v = getValue();
        int len = v.length;
        Double[] array = new Double[len];
        for (int i = 0; i < len; i++) {
            array[i] = Double.valueOf(v[i]);
        }
        return array;
    }

    @Override
    public <E> E[] asArray(Class<E> clazz) {
        double[] v = getValue();
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
        double[] v = getValue();
        Map<K, V> map = new LinkedHashMap<>();
        for (int i = 0; i < v.length; i++) {
            map.put(keyClass.cast(i + 1), valueClass.cast(v[i]));
        }
        // why not use Collections.unmodifiableMap(map) here?
        return map;
    }

    @Override
    public String asString() {
        double[] value = getValue();
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
    public ClickHouseDoubleArrayValue copy(boolean deep) {
        if (!deep) {
            return new ClickHouseDoubleArrayValue(getValue());
        }

        double[] value = getValue();
        return new ClickHouseDoubleArrayValue(Arrays.copyOf(value, value.length));
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
    public ClickHouseDoubleArrayValue resetToDefault() {
        set(ClickHouseValues.EMPTY_DOUBLE_ARRAY);
        return this;
    }

    @Override
    public ClickHouseDoubleArrayValue resetToNullOrEmpty() {
        return resetToDefault();
    }

    @Override
    public String toSqlExpression() {
        return asString();
    }

    @Override
    public ClickHouseDoubleArrayValue update(boolean[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        double[] v = new double[len];
        for (int i = 0; i < len; i++) {
            v[i] = value[i] ? 1D : 0D;
        }
        return set(v);
    }

    @Override
    public ClickHouseDoubleArrayValue update(char[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        double[] v = new double[len];
        for (int i = 0; i < len; i++) {
            v[i] = value[i];
        }
        return set(v);
    }

    @Override
    public ClickHouseDoubleArrayValue update(byte value) {
        return set(new double[] { value });
    }

    @Override
    public ClickHouseDoubleArrayValue update(byte[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        double[] v = new double[len];
        for (int i = 0; i < len; i++) {
            v[i] = value[i];
        }
        return set(v);
    }

    @Override
    public ClickHouseDoubleArrayValue update(short value) {
        return set(new double[] { value });
    }

    @Override
    public ClickHouseDoubleArrayValue update(short[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        double[] v = new double[len];
        for (int i = 0; i < len; i++) {
            v[i] = value[i];
        }
        return set(v);
    }

    @Override
    public ClickHouseDoubleArrayValue update(int value) {
        return set(new double[] { value });
    }

    @Override
    public ClickHouseDoubleArrayValue update(int[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        double[] v = new double[len];
        for (int i = 0; i < len; i++) {
            v[i] = value[i];
        }
        return set(v);
    }

    @Override
    public ClickHouseDoubleArrayValue update(long value) {
        return set(new double[] { value });
    }

    @Override
    public ClickHouseDoubleArrayValue update(long[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        double[] v = new double[len];
        for (int i = 0; i < len; i++) {
            v[i] = value[i];
        }
        return set(v);
    }

    @Override
    public ClickHouseDoubleArrayValue update(float value) {
        return set(new double[] { value });
    }

    @Override
    public ClickHouseDoubleArrayValue update(float[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        double[] v = new double[len];
        for (int i = 0; i < len; i++) {
            v[i] = value[i];
        }
        return set(v);
    }

    @Override
    public ClickHouseDoubleArrayValue update(double value) {
        return set(new double[] { value });
    }

    @Override
    public ClickHouseDoubleArrayValue update(double[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        return set(value);
    }

    @Override
    public ClickHouseDoubleArrayValue update(BigInteger value) {
        return set(value == null ? ClickHouseValues.EMPTY_DOUBLE_ARRAY : new double[] { value.doubleValue() });
    }

    @Override
    public ClickHouseDoubleArrayValue update(BigDecimal value) {
        return set(value == null ? ClickHouseValues.EMPTY_DOUBLE_ARRAY : new double[] { value.doubleValue() });
    }

    @Override
    public ClickHouseDoubleArrayValue update(Enum<?> value) {
        return set(value == null ? ClickHouseValues.EMPTY_DOUBLE_ARRAY : new double[] { value.ordinal() });
    }

    @Override
    public ClickHouseDoubleArrayValue update(Inet4Address value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        throw newUnsupportedException(ClickHouseValues.TYPE_IPV4, TYPE_NAME);
    }

    @Override
    public ClickHouseDoubleArrayValue update(Inet6Address value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        throw newUnsupportedException(ClickHouseValues.TYPE_IPV6, TYPE_NAME);
    }

    @Override
    public ClickHouseDoubleArrayValue update(LocalDate value) {
        return set(value == null ? ClickHouseValues.EMPTY_DOUBLE_ARRAY : new double[] { value.toEpochDay() });
    }

    @Override
    public ClickHouseDoubleArrayValue update(LocalTime value) {
        return set(value == null ? ClickHouseValues.EMPTY_DOUBLE_ARRAY : new double[] { value.toSecondOfDay() });
    }

    @Override
    public ClickHouseDoubleArrayValue update(LocalDateTime value) {
        return set(value == null ? ClickHouseValues.EMPTY_DOUBLE_ARRAY
                : new double[] { value.toEpochSecond(ZoneOffset.UTC) });
    }

    @Override
    public ClickHouseDoubleArrayValue update(Collection<?> value) {
        int size = value == null ? 0 : value.size();
        if (size == 0) {
            return resetToNullOrEmpty();
        }

        double[] v = new double[size];
        int index = 0;
        for (Object o : value) {
            v[index++] = o == null ? 0 : ((Number) o).doubleValue();
        }
        return set(v);
    }

    @Override
    public ClickHouseDoubleArrayValue update(Enumeration<?> value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        List<Number> v = new LinkedList<>();
        while (value.hasMoreElements()) {
            v.add((Number) value.nextElement());
        }

        double[] values = new double[v.size()];
        int index = 0;
        for (Number n : v) {
            values[index++] = n == null ? 0 : n.doubleValue();
        }
        return set(values);
    }

    @Override
    public ClickHouseDoubleArrayValue update(Map<?, ?> value) {
        int size = value == null ? 0 : value.size();
        if (size == 0) {
            return resetToNullOrEmpty();
        }

        double[] v = new double[size];
        int index = 0;
        for (Entry<?, ?> e : value.entrySet()) {
            Object o = e.getValue();
            v[index++] = o == null ? 0 : ((Number) e.getValue()).doubleValue();
        }
        return set(v);
    }

    @Override
    public ClickHouseDoubleArrayValue update(String value) {
        if (ClickHouseChecker.isNullOrBlank(value)) {
            set(ClickHouseValues.EMPTY_DOUBLE_ARRAY);
        } else {
            List<String> list = ClickHouseUtils.readValueArray(value, 0, value.length());
            if (list.isEmpty()) {
                set(ClickHouseValues.EMPTY_DOUBLE_ARRAY);
            } else {
                double[] arr = new double[list.size()];
                int index = 0;
                for (String v : list) {
                    arr[index++] = v == null ? 0D : Double.parseDouble(v);
                }
                set(arr);
            }
        }
        return this;
    }

    @Override
    public ClickHouseDoubleArrayValue update(UUID value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        throw newUnsupportedException(ClickHouseValues.TYPE_UUID, TYPE_NAME);
    }

    @Override
    public ClickHouseDoubleArrayValue update(ClickHouseValue value) {
        if (value == null || value.isNullOrEmpty()) {
            return resetToNullOrEmpty();
        } else if (value instanceof ClickHouseDoubleArrayValue) {
            set(((ClickHouseDoubleArrayValue) value).getValue());
        } else {
            update(value.asArray());
        }
        return this;
    }

    @Override
    public ClickHouseDoubleArrayValue update(Object[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        } else {
            double[] values = new double[len];
            for (int i = 0; i < len; i++) {
                Object o = value[i];
                values[i] = o == null ? 0 : ((Number) o).doubleValue();
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
            return set(new double[] { ((Number) value).doubleValue() });
        } else {
            throw newUnsupportedException(value.getClass().getName(), TYPE_NAME);
        }
    }

    @Override
    public ClickHouseDoubleArrayValue update(Object value) {
        if (value instanceof double[]) {
            set((double[]) value);
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

        return Arrays.equals(getValue(), ((ClickHouseDoubleArrayValue) obj).getValue());
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
            set(new double[length]);
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
        getValue()[index] = value.asDouble();
        return this;
    }
}

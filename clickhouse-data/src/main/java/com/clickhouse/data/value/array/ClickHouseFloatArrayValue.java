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
 * Wrapper of {@code float[]}.
 */
@Deprecated
public class ClickHouseFloatArrayValue extends ClickHouseObjectValue<float[]> implements ClickHouseArraySequence {
    private static final String TYPE_NAME = "float[]";

    /**
     * Creates an empty array.
     *
     * @return empty array
     */

    public static ClickHouseFloatArrayValue ofEmpty() {
        return of(ClickHouseValues.EMPTY_FLOAT_ARRAY);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ClickHouseFloatArrayValue of(float[] value) {
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

    public static ClickHouseFloatArrayValue of(ClickHouseValue ref, float[] value) {
        return ref instanceof ClickHouseFloatArrayValue ? ((ClickHouseFloatArrayValue) ref).set(value)
                : new ClickHouseFloatArrayValue(value);
    }

    protected ClickHouseFloatArrayValue(float[] value) {
        super(value);
    }

    @Override
    protected ClickHouseFloatArrayValue set(float[] value) {
        super.set(ClickHouseChecker.nonNull(value, ClickHouseValues.TYPE_ARRAY));
        return this;
    }

    @Override
    public Object[] asArray() {
        float[] v = getValue();
        int len = v.length;
        Float[] array = new Float[len];
        for (int i = 0; i < len; i++) {
            array[i] = Float.valueOf(v[i]);
        }
        return array;
    }

    @Override
    public <E> E[] asArray(Class<E> clazz) {
        float[] v = getValue();
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
        float[] v = getValue();
        Map<K, V> map = new LinkedHashMap<>();
        for (int i = 0; i < v.length; i++) {
            map.put(keyClass.cast(i + 1), valueClass.cast(v[i]));
        }
        // why not use Collections.unmodifiableMap(map) here?
        return map;
    }

    @Override
    public String asString() {
        float[] value = getValue();
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
    public ClickHouseFloatArrayValue copy(boolean deep) {
        if (!deep) {
            return new ClickHouseFloatArrayValue(getValue());
        }

        float[] value = getValue();
        return new ClickHouseFloatArrayValue(Arrays.copyOf(value, value.length));
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
    public ClickHouseFloatArrayValue resetToDefault() {
        set(ClickHouseValues.EMPTY_FLOAT_ARRAY);
        return this;
    }

    @Override
    public ClickHouseFloatArrayValue resetToNullOrEmpty() {
        return resetToDefault();
    }

    @Override
    public String toSqlExpression() {
        return asString();
    }

    @Override
    public ClickHouseFloatArrayValue update(boolean[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        float[] v = new float[len];
        for (int i = 0; i < len; i++) {
            v[i] = value[i] ? 1F : (float) 0;
        }
        return set(v);
    }

    @Override
    public ClickHouseFloatArrayValue update(char[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        float[] v = new float[len];
        for (int i = 0; i < len; i++) {
            v[i] = value[i];
        }
        return set(v);
    }

    @Override
    public ClickHouseFloatArrayValue update(byte value) {
        return set(new float[] { value });
    }

    @Override
    public ClickHouseFloatArrayValue update(byte[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        float[] v = new float[len];
        for (int i = 0; i < len; i++) {
            v[i] = value[i];
        }
        return set(v);
    }

    @Override
    public ClickHouseFloatArrayValue update(short value) {
        return set(new float[] { value });
    }

    @Override
    public ClickHouseFloatArrayValue update(short[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        float[] v = new float[len];
        for (int i = 0; i < len; i++) {
            v[i] = value[i];
        }
        return set(v);
    }

    @Override
    public ClickHouseFloatArrayValue update(int value) {
        return set(new float[] { value });
    }

    @Override
    public ClickHouseFloatArrayValue update(int[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        float[] v = new float[len];
        for (int i = 0; i < len; i++) {
            v[i] = value[i];
        }
        return set(v);
    }

    @Override
    public ClickHouseFloatArrayValue update(long value) {
        return set(new float[] { value });
    }

    @Override
    public ClickHouseFloatArrayValue update(long[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        float[] v = new float[len];
        for (int i = 0; i < len; i++) {
            v[i] = value[i];
        }
        return set(v);
    }

    @Override
    public ClickHouseFloatArrayValue update(float value) {
        return set(new float[] { value });
    }

    @Override
    public ClickHouseFloatArrayValue update(float[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        return set(value);
    }

    @Override
    public ClickHouseFloatArrayValue update(double value) {
        return set(new float[] { (float) value });
    }

    @Override
    public ClickHouseFloatArrayValue update(double[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        float[] v = new float[len];
        for (int i = 0; i < len; i++) {
            v[i] = (float) value[i];
        }
        return set(v);
    }

    @Override
    public ClickHouseFloatArrayValue update(BigInteger value) {
        return set(value == null ? ClickHouseValues.EMPTY_FLOAT_ARRAY : new float[] { value.floatValue() });
    }

    @Override
    public ClickHouseFloatArrayValue update(BigDecimal value) {
        return set(value == null ? ClickHouseValues.EMPTY_FLOAT_ARRAY : new float[] { value.floatValue() });
    }

    @Override
    public ClickHouseFloatArrayValue update(Enum<?> value) {
        return set(value == null ? ClickHouseValues.EMPTY_FLOAT_ARRAY : new float[] { value.ordinal() });
    }

    @Override
    public ClickHouseFloatArrayValue update(Inet4Address value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        throw newUnsupportedException(ClickHouseValues.TYPE_IPV4, TYPE_NAME);
    }

    @Override
    public ClickHouseFloatArrayValue update(Inet6Address value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        throw newUnsupportedException(ClickHouseValues.TYPE_IPV6, TYPE_NAME);
    }

    @Override
    public ClickHouseFloatArrayValue update(LocalDate value) {
        return set(value == null ? ClickHouseValues.EMPTY_FLOAT_ARRAY : new float[] { value.toEpochDay() });
    }

    @Override
    public ClickHouseFloatArrayValue update(LocalTime value) {
        return set(value == null ? ClickHouseValues.EMPTY_FLOAT_ARRAY : new float[] { value.toSecondOfDay() });
    }

    @Override
    public ClickHouseFloatArrayValue update(LocalDateTime value) {
        return set(value == null ? ClickHouseValues.EMPTY_FLOAT_ARRAY
                : new float[] { value.toEpochSecond(ZoneOffset.UTC) });
    }

    @Override
    public ClickHouseFloatArrayValue update(Collection<?> value) {
        int size = value == null ? 0 : value.size();
        if (size == 0) {
            return resetToNullOrEmpty();
        }

        float[] v = new float[size];
        int index = 0;
        for (Object o : value) {
            v[index++] = o == null ? 0 : ((Number) o).floatValue();
        }
        return set(v);
    }

    @Override
    public ClickHouseFloatArrayValue update(Enumeration<?> value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        List<Number> v = new LinkedList<>();
        while (value.hasMoreElements()) {
            v.add((Number) value.nextElement());
        }

        float[] values = new float[v.size()];
        int index = 0;
        for (Number n : v) {
            values[index++] = n == null ? 0 : n.floatValue();
        }
        return set(values);
    }

    @Override
    public ClickHouseFloatArrayValue update(Map<?, ?> value) {
        int size = value == null ? 0 : value.size();
        if (size == 0) {
            return resetToNullOrEmpty();
        }

        float[] v = new float[size];
        int index = 0;
        for (Entry<?, ?> e : value.entrySet()) {
            Object o = e.getValue();
            v[index++] = o == null ? 0 : ((Number) e.getValue()).floatValue();
        }
        return set(v);
    }

    @Override
    public ClickHouseFloatArrayValue update(String value) {
        if (ClickHouseChecker.isNullOrBlank(value)) {
            set(ClickHouseValues.EMPTY_FLOAT_ARRAY);
        } else {
            List<String> list = ClickHouseUtils.readValueArray(value, 0, value.length());
            if (list.isEmpty()) {
                set(ClickHouseValues.EMPTY_FLOAT_ARRAY);
            } else {
                float[] arr = new float[list.size()];
                int index = 0;
                for (String v : list) {
                    arr[index++] = v == null ? 0F : Float.parseFloat(v);
                }
                set(arr);
            }
        }
        return this;
    }

    @Override
    public ClickHouseFloatArrayValue update(UUID value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        throw newUnsupportedException(ClickHouseValues.TYPE_UUID, TYPE_NAME);
    }

    @Override
    public ClickHouseFloatArrayValue update(ClickHouseValue value) {
        if (value == null || value.isNullOrEmpty()) {
            return resetToNullOrEmpty();
        } else if (value instanceof ClickHouseFloatArrayValue) {
            set(((ClickHouseFloatArrayValue) value).getValue());
        } else {
            update(value.asArray());
        }
        return this;
    }

    @Override
    public ClickHouseFloatArrayValue update(Object[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        } else {
            float[] values = new float[len];
            for (int i = 0; i < len; i++) {
                Object o = value[i];
                values[i] = o == null ? 0 : ((Number) o).floatValue();
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
            return set(new float[] { ((Number) value).floatValue() });
        } else {
            throw newUnsupportedException(value.getClass().getName(), TYPE_NAME);
        }
    }

    @Override
    public ClickHouseFloatArrayValue update(Object value) {
        if (value instanceof float[]) {
            set((float[]) value);
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

        return Arrays.equals(getValue(), ((ClickHouseFloatArrayValue) obj).getValue());
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
            set(new float[length]);
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
        getValue()[index] = value.asFloat();
        return this;
    }
}

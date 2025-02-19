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
 * Wrapper of {@code boolean[]}.
 */
@Deprecated
public class ClickHouseBoolArrayValue extends ClickHouseObjectValue<boolean[]> implements ClickHouseArraySequence {
    private static final String TYPE_NAME = "boolean[]";

    /**
     * Creates a new instance representing empty {@code Bool} array.
     *
     * @return new instance representing an empty array
     */
    public static ClickHouseBoolArrayValue ofEmpty() {
        return of(null, ClickHouseValues.EMPTY_BOOL_ARRAY);
    }

    /**
     * Wraps the given {@code Bool} array.
     *
     * @param value value
     * @return object representing the value
     */
    public static ClickHouseBoolArrayValue of(boolean[] value) {
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

    public static ClickHouseBoolArrayValue of(ClickHouseValue ref, boolean[] value) {
        return ref instanceof ClickHouseBoolArrayValue ? ((ClickHouseBoolArrayValue) ref).set(value)
                : new ClickHouseBoolArrayValue(value);
    }

    protected ClickHouseBoolArrayValue(boolean[] value) {
        super(value);
    }

    @Override
    protected final ClickHouseBoolArrayValue set(boolean[] value) {
        super.set(ClickHouseChecker.nonNull(value, ClickHouseValues.TYPE_ARRAY));
        return this;
    }

    @Override
    public Object[] asArray() {
        boolean[] v = getValue();
        int len = v.length;
        Boolean[] array = new Boolean[len];
        for (int i = 0; i < len; i++) {
            array[i] = Boolean.valueOf(v[i]);
        }
        return array;
    }

    @Override
    public <E> E[] asArray(Class<E> clazz) {
        boolean[] v = getValue();
        int len = v.length;
        if (clazz == Boolean.class) {
            Boolean[] array = new Boolean[len];
            for (int i = 0; i < len; i++) {
                array[i] = Boolean.valueOf(v[i]);
            }
            return (E[]) array;
        } else {
            E[] array = ClickHouseValues.createObjectArray(clazz, len, 1);
            for (int i = 0; i < len; i++) {
                array[i] = clazz.cast(v[i] ? (byte) 1 : (byte) 0);
            }
            return array;
        }
    }

    @Override
    public <K, V> Map<K, V> asMap(Class<K> keyClass, Class<V> valueClass) {
        if (keyClass == null || valueClass == null) {
            throw new IllegalArgumentException("Non-null key and value classes are required");
        }
        boolean[] v = getValue();
        Map<K, V> map = new LinkedHashMap<>();
        for (int i = 0; i < v.length; i++) {
            map.put(keyClass.cast(i + 1), valueClass.cast(v[i]));
        }
        // why not use Collections.unmodifiableMap(map) here?
        return map;
    }

    @Override
    public String asString() {
        boolean[] value = getValue();
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
    public ClickHouseBoolArrayValue copy(boolean deep) {
        if (!deep) {
            return new ClickHouseBoolArrayValue(getValue());
        }

        boolean[] value = getValue();
        return new ClickHouseBoolArrayValue(Arrays.copyOf(value, value.length));
    }

    @Override
    public final boolean isNullable() {
        return false;
    }

    @Override
    public final boolean isNullOrEmpty() {
        return getValue().length == 0;
    }

    @Override
    public ClickHouseBoolArrayValue resetToDefault() {
        set(ClickHouseValues.EMPTY_BOOL_ARRAY);
        return this;
    }

    @Override
    public ClickHouseBoolArrayValue resetToNullOrEmpty() {
        return resetToDefault();
    }

    @Override
    public String toSqlExpression() {
        return asString();
    }

    @Override
    public ClickHouseBoolArrayValue update(byte[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        boolean[] v = new boolean[len];
        for (int i = 0; i < len; i++) {
            v[i] = value[i] == (byte) 1;
        }
        return set(v);
    }

    @Override
    public ClickHouseBoolArrayValue update(char[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        boolean[] v = new boolean[len];
        for (int i = 0; i < len; i++) {
            char ch = value[i];
            v[i] = ch == 'Y' || ch == 'y' || ch == '\001';
        }
        return set(v);
    }

    @Override
    public ClickHouseBoolArrayValue update(byte value) {
        return set(new boolean[] { value == (byte) 1 });
    }

    @Override
    public ClickHouseBoolArrayValue update(boolean[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        return set(value);
    }

    @Override
    public ClickHouseBoolArrayValue update(short value) {
        return set(new boolean[] { value == (short) 1 });
    }

    @Override
    public ClickHouseBoolArrayValue update(short[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        boolean[] v = new boolean[len];
        for (int i = 0; i < len; i++) {
            v[i] = value[i] == (short) 1;
        }
        return set(v);
    }

    @Override
    public ClickHouseBoolArrayValue update(int value) {
        return set(new boolean[] { value == 1 });
    }

    @Override
    public ClickHouseBoolArrayValue update(int[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        boolean[] v = new boolean[len];
        for (int i = 0; i < len; i++) {
            v[i] = value[i] == 1;
        }
        return set(v);
    }

    @Override
    public ClickHouseBoolArrayValue update(long value) {
        return set(new boolean[] { value == 1L });
    }

    @Override
    public ClickHouseBoolArrayValue update(long[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        boolean[] v = new boolean[len];
        for (int i = 0; i < len; i++) {
            v[i] = value[i] == 1L;
        }
        return set(v);
    }

    @Override
    public ClickHouseBoolArrayValue update(float value) {
        return set(new boolean[] { value == 1F });
    }

    @Override
    public ClickHouseBoolArrayValue update(float[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        boolean[] v = new boolean[len];
        for (int i = 0; i < len; i++) {
            v[i] = value[i] == 1F;
        }
        return set(v);
    }

    @Override
    public ClickHouseBoolArrayValue update(double value) {
        return set(new boolean[] { value == 1D });
    }

    @Override
    public ClickHouseBoolArrayValue update(double[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        boolean[] v = new boolean[len];
        for (int i = 0; i < len; i++) {
            v[i] = value[i] == 1D;
        }
        return set(v);
    }

    @Override
    public ClickHouseBoolArrayValue update(BigInteger value) {
        return set(value == null ? ClickHouseValues.EMPTY_BOOL_ARRAY : new boolean[] { BigInteger.ONE.equals(value) });
    }

    @Override
    public ClickHouseBoolArrayValue update(BigDecimal value) {
        return set(value == null ? ClickHouseValues.EMPTY_BOOL_ARRAY : new boolean[] { BigDecimal.ONE.equals(value) });
    }

    @Override
    public ClickHouseBoolArrayValue update(Enum<?> value) {
        return set(value == null ? ClickHouseValues.EMPTY_BOOL_ARRAY : new boolean[] { value.ordinal() == 1 });
    }

    @Override
    public ClickHouseBoolArrayValue update(Inet4Address value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        // return set(value == null ? ClickHouseValues.EMPTY_BOOL_ARRAY :
        // value.getAddress());
        throw newUnsupportedException(ClickHouseValues.TYPE_IPV4, TYPE_NAME);
    }

    @Override
    public ClickHouseBoolArrayValue update(Inet6Address value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        // return set(value == null ? ClickHouseValues.EMPTY_BOOL_ARRAY :
        // value.getAddress());
        throw newUnsupportedException(ClickHouseValues.TYPE_IPV6, TYPE_NAME);
    }

    @Override
    public ClickHouseBoolArrayValue update(LocalDate value) {
        return set(value == null ? ClickHouseValues.EMPTY_BOOL_ARRAY : new boolean[] { value.toEpochDay() == 1L });
    }

    @Override
    public ClickHouseBoolArrayValue update(LocalTime value) {
        return set(value == null ? ClickHouseValues.EMPTY_BOOL_ARRAY : new boolean[] { value.toSecondOfDay() == 1 });
    }

    @Override
    public ClickHouseBoolArrayValue update(LocalDateTime value) {
        return set(value == null ? ClickHouseValues.EMPTY_BOOL_ARRAY
                : new boolean[] { value.toEpochSecond(ZoneOffset.UTC) == 1L });
    }

    @Override
    public ClickHouseBoolArrayValue update(Collection<?> value) {
        int size = value == null ? 0 : value.size();
        if (size == 0) {
            return resetToNullOrEmpty();
        }

        boolean[] v = new boolean[size];
        int index = 0;
        for (Object o : value) {
            v[index++] = o != null && ((Number) o).byteValue() == (byte) 1;
        }
        return set(v);
    }

    @Override
    public ClickHouseBoolArrayValue update(Enumeration<?> value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        List<Number> v = new LinkedList<>();
        while (value.hasMoreElements()) {
            v.add((Number) value.nextElement());
        }

        boolean[] values = new boolean[v.size()];
        int index = 0;
        for (Number n : v) {
            values[index++] = n != null && n.byteValue() == (byte) 1;
        }
        return set(values);
    }

    @Override
    public ClickHouseBoolArrayValue update(Map<?, ?> value) {
        int size = value == null ? 0 : value.size();
        if (size == 0) {
            return resetToNullOrEmpty();
        }

        boolean[] v = new boolean[size];
        int index = 0;
        for (Entry<?, ?> e : value.entrySet()) {
            Object o = e.getValue();
            v[index++] = o != null && ((Number) e.getValue()).byteValue() == (byte) 1;
        }
        return set(v);
    }

    @Override
    public ClickHouseBoolArrayValue update(String value) {
        if (ClickHouseChecker.isNullOrBlank(value)) {
            set(ClickHouseValues.EMPTY_BOOL_ARRAY);
        } else {
            List<String> list = ClickHouseUtils.readValueArray(value, 0, value.length());
            if (list.isEmpty()) {
                set(ClickHouseValues.EMPTY_BOOL_ARRAY);
            } else {
                boolean[] arr = new boolean[list.size()];
                int index = 0;
                for (String v : list) {
                    arr[index++] = v != null && Boolean.parseBoolean(v);
                }
                set(arr);
            }
        }
        return this;
    }

    @Override
    public ClickHouseBoolArrayValue update(UUID value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        // ByteBuffer buffer = ByteBuffer.wrap(new byte[16]);
        // buffer.putLong(value.getMostSignificantBits());
        // buffer.putLong(value.getLeastSignificantBits());
        // return set(buffer.array());
        throw newUnsupportedException(ClickHouseValues.TYPE_UUID, TYPE_NAME);
    }

    @Override
    public ClickHouseBoolArrayValue update(ClickHouseValue value) {
        if (value == null || value.isNullOrEmpty()) {
            return resetToNullOrEmpty();
        } else if (value instanceof ClickHouseBoolArrayValue) {
            set(((ClickHouseBoolArrayValue) value).getValue());
        } else {
            update(value.asArray());
        }
        return this;
    }

    @Override
    public ClickHouseBoolArrayValue update(Object[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        } else if (value instanceof Boolean[]) {
            boolean[] values = new boolean[len];
            for (int i = 0; i < len; i++) {
                values[i] = Boolean.TRUE.equals(value[i]);
            }
            set(values);
        } else {
            boolean[] values = new boolean[len];
            for (int i = 0; i < len; i++) {
                Object o = value[i];
                values[i] = o != null && ((Number) o).byteValue() == (byte) 1;
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
            return set(new boolean[] { ((Number) value).byteValue() == (byte) 1 });
        } else {
            throw newUnsupportedException(value.getClass().getName(), TYPE_NAME);
        }
    }

    @Override
    public ClickHouseBoolArrayValue update(Object value) {
        if (value instanceof boolean[]) {
            set((boolean[]) value);
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

        return Arrays.equals(getValue(), ((ClickHouseBoolArrayValue) obj).getValue());
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
            set(new boolean[length]);
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
        getValue()[index] = value.asBoolean();
        return this;
    }
}

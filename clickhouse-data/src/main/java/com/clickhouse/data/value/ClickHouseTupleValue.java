package com.clickhouse.data.value;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Map.Entry;
import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseValue;
import com.clickhouse.data.ClickHouseValues;

/**
 * Wrapper class of Tuple.
 */
@Deprecated
public class ClickHouseTupleValue extends ClickHouseObjectValue<List<Object>> {
    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ClickHouseTupleValue of(Object... value) {
        return of(null, Arrays.asList(value));
    }

    /**
     * Update value of the given object or create a new instance if {@code ref} is
     * null.
     *
     * @param ref   object to update, could be null
     * @param value value
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ClickHouseTupleValue of(ClickHouseValue ref, List<Object> value) {
        return ref instanceof ClickHouseTupleValue ? (ClickHouseTupleValue) ((ClickHouseTupleValue) ref).set(value)
                : new ClickHouseTupleValue(value);
    }

    protected ClickHouseTupleValue(List<Object> value) {
        super(value);
    }

    protected Object getSingleValue() {
        List<Object> value = getValue();

        if (value == null || value.size() != 1) {
            throw new UnsupportedOperationException("Only singleton tuple supports type conversion");
        }

        return value.iterator().next();
    }

    @Override
    public ClickHouseTupleValue copy(boolean deep) {
        if (!deep || isNullOrEmpty()) {
            return new ClickHouseTupleValue(getValue());
        }

        return new ClickHouseTupleValue(new ArrayList<>(getValue()));
    }

    @Override
    public Object[] asArray() {
        if (isNullOrEmpty()) {
            return ClickHouseValues.EMPTY_OBJECT_ARRAY;
        }

        return getValue().toArray(new Object[0]);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T[] asArray(Class<T> clazz) {
        if (isNullOrEmpty()) {
            return (T[]) ClickHouseValues.EMPTY_OBJECT_ARRAY;
        }

        List<Object> value = getValue();
        T[] array = (T[]) Array.newInstance(ClickHouseChecker.nonNull(clazz, ClickHouseValues.TYPE_CLASS),
                value.size());
        int index = 0;
        for (Object v : value) {
            array[index++] = clazz.cast(v);
        }
        return array;
    }

    @Override
    public Map<Object, Object> asMap() {
        if (isNullOrEmpty()) {
            return Collections.emptyMap();
        }

        Map<Object, Object> map = new LinkedHashMap<>();
        int index = 1;
        for (Object v : getValue()) {
            map.put(index++, v);
        }
        return map;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> Map<K, V> asMap(Class<K> keyClass, Class<V> valueClass) {
        if (isNullOrEmpty()) {
            return Collections.emptyMap();
        }

        // Class.cast() cannot convert Integer to Byte or String, so we're stuck with
        // Integer...
        if (Integer.class != keyClass || valueClass == null) {
            throw new IllegalArgumentException("Key class must be Integer and value class cannot be null");
        }

        Map<Integer, V> map = new LinkedHashMap<>();
        int index = 1;
        for (Object v : getValue()) {
            map.put(index++, valueClass.cast(v));
        }
        return (Map<K, V>) map;
    }

    @Override
    public String asString() {
        return Arrays.deepToString(asArray());
    }

    @Override
    public List<Object> asTuple() {
        return getValue();
    }

    @Override
    public boolean isNullable() {
        return false;
    }

    @Override
    public boolean isNullOrEmpty() {
        return getValue().isEmpty();
    }

    @Override
    public ClickHouseTupleValue resetToDefault() {
        set(Collections.emptyList());
        return this;
    }

    @Override
    public ClickHouseTupleValue resetToNullOrEmpty() {
        return resetToDefault();
    }

    @Override
    public String toSqlExpression() {
        StringBuilder builder = new StringBuilder().append('(');
        for (Object v : getValue()) {
            builder.append(ClickHouseValues.convertToSqlExpression(v)).append(',');
        }
        if (builder.length() > 1) {
            builder.setLength(builder.length() - 1);
        }
        return builder.append(')').toString();
    }

    @Override
    public ClickHouseTupleValue update(boolean[] value) {
        if (value == null || value.length == 0) {
            set(Collections.emptyList());
            return this;
        }

        List<Object> v = new ArrayList<>(value.length);
        for (boolean b : value) {
            v.add(b ? (byte) 1 : (byte) 0);
        }
        set(v);
        return this;
    }

    @Override
    public ClickHouseTupleValue update(char[] value) {
        if (value == null || value.length == 0) {
            set(Collections.emptyList());
            return this;
        }

        List<Object> v = new ArrayList<>(value.length);
        for (char c : value) {
            v.add((int) c);
        }
        set(v);
        return this;
    }

    @Override
    public ClickHouseTupleValue update(byte value) {
        set(Collections.singletonList(value));
        return this;
    }

    @Override
    public ClickHouseTupleValue update(byte[] value) {
        if (value == null || value.length == 0) {
            set(Collections.emptyList());
            return this;
        }

        List<Object> v = new ArrayList<>(value.length);
        for (byte b : value) {
            v.add(b);
        }
        set(v);
        return this;
    }

    @Override
    public ClickHouseTupleValue update(short value) {
        set(Collections.singletonList(value));
        return this;
    }

    @Override
    public ClickHouseTupleValue update(short[] value) {
        if (value == null || value.length == 0) {
            set(Collections.emptyList());
            return this;
        }

        List<Object> v = new ArrayList<>(value.length);
        for (short s : value) {
            v.add(s);
        }
        set(v);
        return this;
    }

    @Override
    public ClickHouseTupleValue update(int value) {
        set(Collections.singletonList(value));
        return this;
    }

    @Override
    public ClickHouseTupleValue update(int[] value) {
        if (value == null || value.length == 0) {
            set(Collections.emptyList());
            return this;
        }

        List<Object> v = new ArrayList<>(value.length);
        for (int i : value) {
            v.add(i);
        }
        set(v);
        return this;
    }

    @Override
    public ClickHouseTupleValue update(long value) {
        set(Collections.singletonList(value));
        return this;
    }

    @Override
    public ClickHouseTupleValue update(long[] value) {
        if (value == null || value.length == 0) {
            set(Collections.emptyList());
            return this;
        }

        List<Object> v = new ArrayList<>(value.length);
        for (long l : value) {
            v.add(l);
        }
        set(v);
        return this;
    }

    @Override
    public ClickHouseTupleValue update(float value) {
        set(Collections.singletonList(value));
        return this;
    }

    @Override
    public ClickHouseTupleValue update(float[] value) {
        if (value == null || value.length == 0) {
            set(Collections.emptyList());
            return this;
        }

        List<Object> v = new ArrayList<>(value.length);
        for (float f : value) {
            v.add(f);
        }
        set(v);
        return this;
    }

    @Override
    public ClickHouseTupleValue update(double value) {
        set(Collections.singletonList(value));
        return this;
    }

    @Override
    public ClickHouseTupleValue update(double[] value) {
        if (value == null || value.length == 0) {
            set(Collections.emptyList());
            return this;
        }

        List<Object> v = new ArrayList<>(value.length);
        for (double d : value) {
            v.add(d);
        }
        set(v);
        return this;
    }

    @Override
    public ClickHouseTupleValue update(BigInteger value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }
        set(Collections.singletonList(value));
        return this;
    }

    @Override
    public ClickHouseTupleValue update(BigDecimal value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }
        set(Collections.singletonList(value));
        return this;
    }

    @Override
    public ClickHouseTupleValue update(Enum<?> value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }
        set(Collections.singletonList(value));
        return this;
    }

    @Override
    public ClickHouseTupleValue update(Inet4Address value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }
        set(Collections.singletonList(value));
        return this;
    }

    @Override
    public ClickHouseTupleValue update(Inet6Address value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }
        set(Collections.singletonList(value));
        return this;
    }

    @Override
    public ClickHouseTupleValue update(LocalDate value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }
        set(Collections.singletonList(value));
        return this;
    }

    @Override
    public ClickHouseTupleValue update(LocalTime value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }
        set(Collections.singletonList(value));
        return this;
    }

    @Override
    public ClickHouseTupleValue update(LocalDateTime value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }
        set(Collections.singletonList(value));
        return this;
    }

    @Override
    public ClickHouseTupleValue update(Collection<?> value) {
        int size = value == null ? 0 : value.size();
        if (size == 0) {
            set(Collections.emptyList());
            return this;
        }

        set(new ArrayList<>(value));
        return this;
    }

    @Override
    public ClickHouseTupleValue update(Enumeration<?> value) {
        if (value == null) {
            set(Collections.emptyList());
            return this;
        }

        List<Object> v = new LinkedList<>();
        while (value.hasMoreElements()) {
            v.add(value.nextElement());
        }
        set(v);
        return this;
    }

    @Override
    public ClickHouseTupleValue update(Map<?, ?> value) {
        int size = value == null ? 0 : value.size();
        if (size == 0) {
            set(Collections.emptyList());
            return this;
        }

        List<Object> v = new ArrayList<>(size);
        for (Entry<?, ?> e : value.entrySet()) {
            v.add(e.getValue());
        }
        set(v);
        return this;
    }

    @Override
    public ClickHouseTupleValue update(String value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else if (value.isEmpty() || ClickHouseValues.EMPTY_TUPLE_EXPR.equals(value)) {
            resetToDefault();
        } else {
            // TODO parse string
            set(Collections.singletonList(value));
        }
        return this;
    }

    @Override
    public ClickHouseTupleValue update(UUID value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }
        set(Collections.singletonList(value));
        return this;
    }

    @Override
    public ClickHouseTupleValue update(ClickHouseValue value) {
        if (value == null || value.isNullOrEmpty()) {
            set(Collections.emptyList());
            return this;
        }

        set(value.asTuple());
        return this;
    }

    @Override
    public ClickHouseTupleValue update(Object[] value) {
        if (value == null || value.length == 0) {
            set(Collections.emptyList());
            return this;
        }

        set(Arrays.asList(value));
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ClickHouseTupleValue update(Object value) {
        if (value instanceof List) {
            set((List<Object>) value);
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

        List<Object> v1 = getValue();
        List<Object> v2 = ((ClickHouseTupleValue) obj).getValue();
        return v1 == v2 || (v1 != null && v1.equals(v2)); // deep equal?
    }

    @Override
    public int hashCode() {
        List<Object> v = getValue();
        return Arrays.deepHashCode(v == null ? null : v.toArray(new Object[v.size()]));
    }
}

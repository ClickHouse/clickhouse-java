package com.clickhouse.data.value;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseValue;
import com.clickhouse.data.ClickHouseValues;

/**
 * Wrapper class of Nested.
 */
@Deprecated
public class ClickHouseNestedValue extends ClickHouseObjectValue<Object[][]> {
    /**
     * Creates an empty nested value.
     *
     * @param columns non-null columns
     * @return empty nested value
     */
    public static ClickHouseNestedValue ofEmpty(List<ClickHouseColumn> columns) {
        return of(null, columns, new Object[0][]);
    }

    /**
     * Wrap the given value.
     *
     * @param columns columns
     * @param values  values
     * @return object representing the value
     */
    public static ClickHouseNestedValue of(List<ClickHouseColumn> columns, Object[][] values) {
        return of(null, columns, values);
    }

    /**
     * Update value of the given object or create a new instance if {@code ref} is
     * null.
     *
     * @param ref     object to update, could be null
     * @param columns columns
     * @param values  values
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ClickHouseNestedValue of(ClickHouseValue ref, List<ClickHouseColumn> columns, Object[][] values) {
        return ref instanceof ClickHouseNestedValue
                ? ((ClickHouseNestedValue) ref).set(values != null ? values : ClickHouseValues.EMPTY_OBJECT_ARRAY2)
                : new ClickHouseNestedValue(columns, values);
    }

    protected static Object[][] check(List<ClickHouseColumn> columns, Object[][] value) {
        if (columns == null || value == null) {
            throw new IllegalArgumentException("Non-null columns and value are required");
        }

        int size = columns.size();
        for (int i = 0, len = value.length; i < len; i++) {
            Object[] objs = value[i];
            if (objs == null || objs.length != size) {
                throw new IllegalArgumentException("Columns and values should have same length");
            }
        }

        return value;
    }

    private final List<ClickHouseColumn> columns;

    protected ClickHouseNestedValue(List<ClickHouseColumn> columns, Object[][] values) {
        super(check(columns, values));
        this.columns = columns;
    }

    protected Object getSingleValue() {
        Object[][] value = getValue();

        if (value == null || value.length != 1 || value[0] == null || value[0].length != 1) {
            throw new UnsupportedOperationException(
                    "Only nested object containing only one value(one column and one row) supports type conversion");
        }

        return value[0][0];
    }

    @Override
    protected ClickHouseNestedValue set(Object[][] value) {
        if (columns == null && getValue() == null) { // must be called from constructor
            super.set(value);
        } else {
            super.set(check(columns, value));
        }
        return this;
    }

    /**
     * Gets immutable list of nested columns.
     *
     * @return immutable list of columns
     */
    public List<ClickHouseColumn> getColumns() {
        return Collections.unmodifiableList(columns);
    }

    @Override
    public ClickHouseNestedValue copy(boolean deep) {
        if (!deep) {
            return new ClickHouseNestedValue(columns, getValue());
        }

        Object[][] value = getValue();
        int len = value.length;
        Object[][] newValue = new Object[len][];
        for (int i = 0; i < len; i++) {
            Object[] v = value[i];
            newValue[i] = Arrays.copyOf(v, v.length);
        }
        return new ClickHouseNestedValue(columns, newValue);
    }

    @Override
    public Object[] asArray() {
        return getValue();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T[] asArray(Class<T> clazz) {
        Object[][] v = getValue();
        T[] array = (T[]) Array.newInstance(ClickHouseChecker.nonNull(clazz, ClickHouseValues.TYPE_CLASS), v.length);
        int index = 0;
        for (Object[] o : v) {
            array[index++] = clazz.cast(o);
        }
        return array;
    }

    @Override
    public Map<Object, Object> asMap() {
        Object[][] values = getValue();
        int len = values.length;
        if (len == 0) {
            return Collections.emptyMap();
        }

        Map<Object, Object> map = new LinkedHashMap<>();
        for (int i = 0, size = columns.size(); i < size; i++) {
            Object[] v = new Object[len];
            for (int j = 0; j < len; j++) {
                v[j] = values[j][i];
            }
            map.put(columns.get(i).getColumnName(), v);
        }
        // why not use Collections.unmodifiableMap(map) here?
        return map;
    }

    @Override
    public <K, V> Map<K, V> asMap(Class<K> keyClass, Class<V> valueClass) {
        if (keyClass == null || valueClass == null) {
            throw new IllegalArgumentException("Non-null key and value classes are required");
        }
        Object[][] values = getValue();
        int len = values.length;
        if (len == 0) {
            return Collections.emptyMap();
        }

        Map<K, V> map = new LinkedHashMap<>();
        int size = columns.size();
        if (len == 1 && !valueClass.isArray()) {
            for (int i = 0; i < size; i++) {
                map.put(keyClass.cast(columns.get(i).getColumnName()), valueClass.cast(values[i][0]));
            }
        } else if (!valueClass.isArray()) {
            throw new IllegalArgumentException("Value class should be array");
        }

        Class<?> compClass = valueClass.getComponentType();
        if (compClass == Object.class) {
            for (int i = 0; i < size; i++) {
                Object[] v = new Object[len];
                for (int j = 0; j < len; j++) {
                    v[j] = values[j][i];
                }
                map.put(keyClass.cast(columns.get(i).getColumnName()), valueClass.cast(v));
            }
        } else {
            for (int i = 0; i < size; i++) {
                Object[] v = (Object[]) Array.newInstance(compClass, len);
                for (int j = 0; j < len; j++) {
                    v[j] = compClass.cast(values[j][i]);
                }
                map.put(keyClass.cast(columns.get(i).getColumnName()), valueClass.cast(v));
            }
        }
        // why not use Collections.unmodifiableMap(map) here?
        return map;
    }

    @Override
    public String asString() {
        return Arrays.deepToString(getValue());
    }

    @Override
    public boolean isNullable() {
        return false;
    }

    @Override
    public boolean isNullOrEmpty() {
        Object[][] value = getValue();
        return value == null || value.length == 0;
    }

    @Override
    public ClickHouseNestedValue resetToDefault() {
        set(ClickHouseValues.EMPTY_OBJECT_ARRAY2);
        return this;
    }

    @Override
    public ClickHouseNestedValue resetToNullOrEmpty() {
        return resetToDefault();
    }

    @Override
    public String toSqlExpression() {
        Object[][] value = getValue();
        if (value == null || value.length == 0) {
            return ClickHouseValues.EMPTY_ARRAY_EXPR;
        }

        StringBuilder builder = new StringBuilder();
        for (Object[] v : value) {
            if (v == null || v.length == 0) {
                builder.append(ClickHouseValues.EMPTY_ARRAY_EXPR);
            } else {
                builder.append('[');
                for (Object o : v) {
                    builder.append(ClickHouseValues.convertToSqlExpression(o)).append(',');
                }
                builder.setLength(builder.length() - 1);
                builder.append(']');
            }
            builder.append(',');
        }
        builder.setLength(builder.length() - 1);
        return builder.toString();
    }

    @Override
    public ClickHouseNestedValue update(boolean value) {
        set(new Object[][] { new Byte[] { value ? (byte) 1 : (byte) 0 } });
        return this;
    }

    @Override
    public ClickHouseNestedValue update(boolean[] value) {
        int len = value == null ? 0 : value.length;
        Byte[] v = new Byte[len];
        if (len > 0) {
            int index = 0;
            for (boolean b : value) {
                v[index++] = b ? (byte) 1 : (byte) 0;
            }
        }
        set(new Object[][] { v });
        return this;
    }

    @Override
    public ClickHouseNestedValue update(char value) {
        set(new Object[][] { new Integer[] { (int) value } });
        return this;
    }

    @Override
    public ClickHouseNestedValue update(char[] value) {
        int len = value == null ? 0 : value.length;
        Integer[] v = new Integer[len];
        if (len > 0) {
            int index = 0;
            for (char c : value) {
                v[index++] = (int) c;
            }
        }
        set(new Object[][] { v });
        return this;
    }

    @Override
    public ClickHouseNestedValue update(byte value) {
        set(new Object[][] { new Object[] { value } });
        return this;
    }

    @Override
    public ClickHouseNestedValue update(byte[] value) {
        int len = value == null ? 0 : value.length;
        Byte[] v = new Byte[len];
        if (len > 0) {
            int index = 0;
            for (byte b : value) {
                v[index++] = b;
            }
        }
        set(new Object[][] { v });
        return this;
    }

    @Override
    public ClickHouseNestedValue update(short value) {
        set(new Object[][] { new Short[] { value } });
        return this;
    }

    @Override
    public ClickHouseNestedValue update(short[] value) {
        int len = value == null ? 0 : value.length;
        Short[] v = new Short[len];
        if (len > 0) {
            int index = 0;
            for (short s : value) {
                v[index++] = s;
            }
        }
        set(new Object[][] { v });
        return this;
    }

    @Override
    public ClickHouseNestedValue update(int value) {
        set(new Object[][] { new Integer[] { value } });
        return this;
    }

    @Override
    public ClickHouseNestedValue update(int[] value) {
        int len = value == null ? 0 : value.length;
        Integer[] v = new Integer[len];
        if (len > 0) {
            int index = 0;
            for (int i : value) {
                v[index++] = i;
            }
        }
        set(new Object[][] { v });
        return this;
    }

    @Override
    public ClickHouseNestedValue update(long value) {
        set(new Object[][] { new Long[] { value } });
        return this;
    }

    @Override
    public ClickHouseNestedValue update(long[] value) {
        int len = value == null ? 0 : value.length;
        Long[] v = new Long[len];
        if (len > 0) {
            int index = 0;
            for (long l : value) {
                v[index++] = l;
            }
        }
        set(new Object[][] { v });
        return this;
    }

    @Override
    public ClickHouseNestedValue update(float value) {
        set(new Object[][] { new Float[] { value } });
        return this;
    }

    @Override
    public ClickHouseNestedValue update(float[] value) {
        int len = value == null ? 0 : value.length;
        Float[] v = new Float[len];
        if (len > 0) {
            int index = 0;
            for (float f : value) {
                v[index++] = f;
            }
        }
        set(new Object[][] { v });
        return this;
    }

    @Override
    public ClickHouseNestedValue update(double value) {
        set(new Object[][] { new Double[] { value } });
        return this;
    }

    @Override
    public ClickHouseNestedValue update(double[] value) {
        int len = value == null ? 0 : value.length;
        Double[] v = new Double[len];
        if (len > 0) {
            int index = 0;
            for (double d : value) {
                v[index++] = d;
            }
        }
        return set(new Object[][] { v });
    }

    @Override
    public ClickHouseNestedValue update(BigInteger value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }
        return set(new Object[][] { new BigInteger[] { value } });
    }

    @Override
    public ClickHouseNestedValue update(BigDecimal value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }
        return set(new Object[][] { new BigDecimal[] { value } });
    }

    @Override
    public ClickHouseNestedValue update(Enum<?> value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }
        return set(new Object[][] { new Enum[] { value } });
    }

    @Override
    public ClickHouseNestedValue update(Inet4Address value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }
        return set(new Object[][] { new Inet4Address[] { value } });
    }

    @Override
    public ClickHouseNestedValue update(Inet6Address value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }
        return set(new Object[][] { new Inet6Address[] { value } });
    }

    @Override
    public ClickHouseNestedValue update(LocalDate value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }
        return set(new Object[][] { new LocalDate[] { value } });
    }

    @Override
    public ClickHouseNestedValue update(LocalTime value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }
        return set(new Object[][] { new LocalTime[] { value } });
    }

    @Override
    public ClickHouseNestedValue update(LocalDateTime value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }
        return set(new Object[][] { new LocalDateTime[] { value } });
    }

    @Override
    public ClickHouseNestedValue update(Collection<?> value) {
        int size = value == null ? 0 : value.size();
        if (size == 0) {
            return resetToNullOrEmpty();
        }

        Object[] v = new Object[size];
        int index = 0;
        for (Object o : value) {
            v[index++] = o;
        }
        return set(new Object[][] { v });
    }

    @Override
    public ClickHouseNestedValue update(Enumeration<?> value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        List<Object> v = new LinkedList<>();
        while (value.hasMoreElements()) {
            v.add(value.nextElement());
        }
        return set(new Object[][] { v.toArray(new Object[v.size()]) });
    }

    @Override
    public ClickHouseNestedValue update(Map<?, ?> value) {
        int size = value == null ? 0 : value.size();
        if (size == 0) {
            return resetToNullOrEmpty();
        }

        Object[] v = new Object[size];
        int index = 0;
        for (Object o : value.values()) {
            v[index++] = o;
        }
        return set(new Object[][] { v });
    }

    @Override
    public ClickHouseNestedValue update(String value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else if (value.isEmpty() || ClickHouseValues.EMPTY_ARRAY_EXPR.equals(value)) {
            resetToDefault();
        } else {
            // TODO parse string
            set(new Object[][] { new String[] { value } });
        }
        return this;
    }

    @Override
    public ClickHouseNestedValue update(UUID value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }
        return set(new Object[][] { new UUID[] { value } });
    }

    @Override
    public ClickHouseNestedValue update(ClickHouseValue value) {
        if (value == null || value.isNullOrEmpty()) {
            return resetToNullOrEmpty();
        } else if (value instanceof ClickHouseNestedValue) {
            set(((ClickHouseNestedValue) value).getValue());
        } else {
            set(new Object[][] { value.asArray() });
        }
        return this;
    }

    @Override
    public ClickHouseNestedValue update(Object[] value) {
        if (value == null) {
            return resetToNullOrEmpty();
        } else if (value instanceof Object[][]) {
            set((Object[][]) value);
        } else {
            set(new Object[][] { value });
        }
        return this;
    }

    @Override
    public ClickHouseValue updateUnknown(Object value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }
        return set(new Object[][] { new Object[] { value } });
    }

    @Override
    public ClickHouseNestedValue update(Object value) {
        if (value instanceof Object[][]) {
            set((Object[][]) value);
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

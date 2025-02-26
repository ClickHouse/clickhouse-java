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
import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseValue;
import com.clickhouse.data.ClickHouseValues;

/**
 * Wrapper class of {@code Ring}.
 */
public class ClickHouseGeoRingValue extends ClickHouseObjectValue<double[][]> {
    static final double[][] EMPTY_VALUE = new double[0][];

    /**
     * Creates an empty ring.
     *
     * @return empty ring
     */
    public static ClickHouseGeoRingValue ofEmpty() {
        return of(null, EMPTY_VALUE);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ClickHouseGeoRingValue of(double[][] value) {
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
    public static ClickHouseGeoRingValue of(ClickHouseValue ref, double[][] value) {
        return ref instanceof ClickHouseGeoRingValue ? ((ClickHouseGeoRingValue) ref).set(value)
                : new ClickHouseGeoRingValue(value);
    }

    protected static double[][] check(double[][] value) {
        for (int i = 0, len = ClickHouseChecker.nonNull(value, ClickHouseValues.TYPE_RING).length; i < len; i++) {
            ClickHouseGeoPointValue.check(value[i]);
        }

        return value;
    }

    protected static String convert(double[][] value) {
        StringBuilder builder = new StringBuilder().append('[');
        for (int i = 0, len = value.length; i < len; i++) {
            builder.append(ClickHouseGeoPointValue.convert(value[i])).append(',');
        }

        if (builder.length() > 1) {
            builder.setLength(builder.length() - 1);
        }
        return builder.append(']').toString();
    }

    protected ClickHouseGeoRingValue(double[][] value) {
        super(value);
    }

    @Override
    protected ClickHouseGeoRingValue set(double[][] value) {
        super.set(check(value));
        return this;
    }

    @Override
    public ClickHouseGeoRingValue copy(boolean deep) {
        if (!deep) {
            return new ClickHouseGeoRingValue(getValue());
        }

        double[][] value = getValue();
        double[][] newValue = new double[value.length][];
        int index = 0;
        for (double[] v : value) {
            newValue[index++] = Arrays.copyOf(v, v.length);
        }
        return new ClickHouseGeoRingValue(newValue);
    }

    @Override
    public Object[] asArray() {
        return getValue();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T[] asArray(Class<T> clazz) {
        double[][] v = getValue();
        T[] array = (T[]) Array.newInstance(ClickHouseChecker.nonNull(clazz, ClickHouseValues.TYPE_CLASS), v.length);
        int index = 0;
        for (double[] d : v) {
            array[index++] = clazz.cast(d);
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
        for (double[] d : getValue()) {
            map.put(keyClass.cast(index++), valueClass.cast(d));
        }
        // why not use Collections.unmodifiableMap(map) here?
        return map;
    }

    @Override
    public String asString() {
        return convert(getValue());
    }

    @Override
    public boolean isNullable() {
        return false;
    }

    @Override
    public boolean isNullOrEmpty() {
        double[][] value = getValue();
        return value == null || value.length == 0;
    }

    @Override
    public ClickHouseGeoRingValue resetToDefault() {
        set(EMPTY_VALUE);
        return this;
    }

    @Override
    public ClickHouseGeoRingValue resetToNullOrEmpty() {
        return resetToDefault();
    }

    @Override
    public String toSqlExpression() {
        return convert(getValue());
    }

    @Override
    public ClickHouseGeoRingValue update(boolean value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_BOOLEAN, ClickHouseValues.TYPE_RING);
    }

    @Override
    public ClickHouseGeoRingValue update(boolean[] value) {
        if (value == null || value.length != 2) {
            throw new IllegalArgumentException(ClickHouseValues.ERROR_INVALID_POINT + Arrays.toString(value));
        }
        set(new double[][] { new double[] { value[0] ? 1 : 0, value[1] ? 0 : 1 } });
        return this;
    }

    @Override
    public ClickHouseGeoRingValue update(char value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_CHAR, ClickHouseValues.TYPE_RING);
    }

    @Override
    public ClickHouseGeoRingValue update(char[] value) {
        if (value == null || value.length != 2) {
            throw new IllegalArgumentException(ClickHouseValues.ERROR_INVALID_POINT + Arrays.toString(value));
        }
        set(new double[][] { new double[] { value[0], value[1] } });
        return this;
    }

    @Override
    public ClickHouseGeoRingValue update(byte value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_BYTE, ClickHouseValues.TYPE_RING);
    }

    @Override
    public ClickHouseGeoRingValue update(byte[] value) {
        if (value == null || value.length != 2) {
            throw new IllegalArgumentException(ClickHouseValues.ERROR_INVALID_POINT + Arrays.toString(value));
        }
        set(new double[][] { new double[] { value[0], value[1] } });
        return this;
    }

    @Override
    public ClickHouseGeoRingValue update(short value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_SHORT, ClickHouseValues.TYPE_RING);
    }

    @Override
    public ClickHouseGeoRingValue update(short[] value) {
        if (value == null || value.length != 2) {
            throw new IllegalArgumentException(ClickHouseValues.ERROR_INVALID_POINT + Arrays.toString(value));
        }
        set(new double[][] { new double[] { value[0], value[1] } });
        return this;
    }

    @Override
    public ClickHouseGeoRingValue update(int value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_INT, ClickHouseValues.TYPE_RING);
    }

    @Override
    public ClickHouseGeoRingValue update(int[] value) {
        if (value == null || value.length != 2) {
            throw new IllegalArgumentException(ClickHouseValues.ERROR_INVALID_POINT + Arrays.toString(value));
        }
        set(new double[][] { new double[] { value[0], value[1] } });
        return this;
    }

    @Override
    public ClickHouseGeoRingValue update(long value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_LONG, ClickHouseValues.TYPE_RING);
    }

    @Override
    public ClickHouseGeoRingValue update(long[] value) {
        if (value == null || value.length != 2) {
            throw new IllegalArgumentException(ClickHouseValues.ERROR_INVALID_POINT + Arrays.toString(value));
        }
        set(new double[][] { new double[] { value[0], value[1] } });
        return this;
    }

    @Override
    public ClickHouseGeoRingValue update(float value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_FLOAT, ClickHouseValues.TYPE_RING);
    }

    @Override
    public ClickHouseGeoRingValue update(float[] value) {
        if (value == null || value.length != 2) {
            throw new IllegalArgumentException(ClickHouseValues.ERROR_INVALID_POINT + Arrays.toString(value));
        }
        set(new double[][] { new double[] { value[0], value[1] } });
        return this;
    }

    @Override
    public ClickHouseGeoRingValue update(double value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_DOUBLE, ClickHouseValues.TYPE_RING);
    }

    @Override
    public ClickHouseGeoRingValue update(double[] value) {
        if (value == null || value.length != 2) {
            throw new IllegalArgumentException(ClickHouseValues.ERROR_INVALID_POINT + Arrays.toString(value));
        }
        set(new double[][] { value });
        return this;
    }

    @Override
    public ClickHouseGeoRingValue update(BigInteger value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_BIG_INTEGER, ClickHouseValues.TYPE_RING);
    }

    @Override
    public ClickHouseGeoRingValue update(BigDecimal value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_BIG_DECIMAL, ClickHouseValues.TYPE_RING);
    }

    @Override
    public ClickHouseGeoRingValue update(Enum<?> value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_ENUM, ClickHouseValues.TYPE_RING);
    }

    @Override
    public ClickHouseGeoRingValue update(Inet4Address value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_IPV4, ClickHouseValues.TYPE_RING);
    }

    @Override
    public ClickHouseGeoRingValue update(Inet6Address value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_IPV6, ClickHouseValues.TYPE_RING);
    }

    @Override
    public ClickHouseGeoRingValue update(LocalDate value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_DATE, ClickHouseValues.TYPE_RING);
    }

    @Override
    public ClickHouseGeoRingValue update(LocalTime value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_TIME, ClickHouseValues.TYPE_RING);
    }

    @Override
    public ClickHouseGeoRingValue update(LocalDateTime value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_DATE_TIME, ClickHouseValues.TYPE_RING);
    }

    @Override
    public ClickHouseGeoRingValue update(Collection<?> value) {
        if (value == null || value.size() != 2) {
            throw new IllegalArgumentException(ClickHouseValues.ERROR_INVALID_POINT + value);
        }
        Iterator<?> i = value.iterator();
        Object v1 = i.next();
        Object v2 = i.next();
        if (v1 instanceof Number) {
            set(new double[][] { new double[] { ((Number) v1).doubleValue(), ((Number) v2).doubleValue() } });
        } else {
            set(new double[][] {
                    new double[] { Double.parseDouble(v1.toString()), Double.parseDouble(v2.toString()) } });
        }
        return this;
    }

    @Override
    public ClickHouseGeoRingValue update(Enumeration<?> value) {
        if (value == null || !value.hasMoreElements()) {
            throw new IllegalArgumentException(ClickHouseValues.ERROR_INVALID_POINT + value);
        }
        Object v1 = value.nextElement();
        if (!value.hasMoreElements()) {
            throw new IllegalArgumentException(ClickHouseValues.ERROR_INVALID_POINT + value);
        }
        Object v2 = value.nextElement();
        if (value.hasMoreElements()) {
            throw new IllegalArgumentException(ClickHouseValues.ERROR_INVALID_POINT + value);
        }

        if (v1 instanceof Number) {
            set(new double[][] { new double[] { ((Number) v1).doubleValue(), ((Number) v2).doubleValue() } });
        } else {
            set(new double[][] {
                    new double[] { Double.parseDouble(v1.toString()), Double.parseDouble(v2.toString()) } });
        }
        return this;
    }

    @Override
    public ClickHouseGeoRingValue update(Map<?, ?> value) {
        if (value == null || value.size() != 2) {
            throw new IllegalArgumentException(ClickHouseValues.ERROR_INVALID_POINT + value);
        }
        Iterator<?> i = value.values().iterator();
        Object v1 = i.next();
        Object v2 = i.next();
        if (v1 instanceof Number) {
            set(new double[][] { new double[] { ((Number) v1).doubleValue(), ((Number) v2).doubleValue() } });
        } else {
            set(new double[][] {
                    new double[] { Double.parseDouble(v1.toString()), Double.parseDouble(v2.toString()) } });
        }
        return this;
    }

    @Override
    public ClickHouseGeoRingValue update(String value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_STRING, ClickHouseValues.TYPE_RING);
    }

    @Override
    public ClickHouseGeoRingValue update(UUID value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_UUID, ClickHouseValues.TYPE_RING);
    }

    @Override
    public ClickHouseGeoRingValue update(ClickHouseValue value) {
        if (value == null || value.isNullOrEmpty()) {
            resetToNullOrEmpty();
        } else if (value instanceof ClickHouseGeoRingValue) {
            set(((ClickHouseGeoRingValue) value).getValue());
        } else {
            update(value.asArray());
        }
        return this;
    }

    @Override
    public ClickHouseGeoRingValue update(Object[] value) {
        if (value instanceof double[][]) {
            return set((double[][]) value);
        } else if (value == null || value.length != 2) {
            throw new IllegalArgumentException(ClickHouseValues.ERROR_INVALID_POINT + Arrays.toString(value));
        }
        Object v1 = value[0];
        Object v2 = value[1];
        if (v1 instanceof Number) {
            set(new double[][] { new double[] { ((Number) v1).doubleValue(), ((Number) v2).doubleValue() } });
        } else {
            set(new double[][] {
                    new double[] { Double.parseDouble(v1.toString()), Double.parseDouble(v2.toString()) } });
        }
        return this;
    }

    @Override
    public ClickHouseGeoRingValue update(Object value) {
        if (value instanceof double[][]) {
            set((double[][]) value);
        } else {
            super.update(value);
        }
        return this;
    }
}

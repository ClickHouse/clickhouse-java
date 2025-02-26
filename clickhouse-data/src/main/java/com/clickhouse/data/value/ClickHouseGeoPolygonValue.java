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
 * Wrapper class of {@code Polygon}.
 */
public class ClickHouseGeoPolygonValue extends ClickHouseObjectValue<double[][][]> {
    static final double[][][] EMPTY_VALUE = new double[0][][];

    /**
     * Creates an empty polygon.
     *
     * @return empty polygon
     */
    public static ClickHouseGeoPolygonValue ofEmpty() {
        return of(null, EMPTY_VALUE);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ClickHouseGeoPolygonValue of(double[][][] value) {
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
    public static ClickHouseGeoPolygonValue of(ClickHouseValue ref, double[][][] value) {
        return ref instanceof ClickHouseGeoPolygonValue ? ((ClickHouseGeoPolygonValue) ref).set(value)
                : new ClickHouseGeoPolygonValue(value);
    }

    protected static double[][][] check(double[][][] value) {
        for (int i = 0, len = ClickHouseChecker.nonNull(value, ClickHouseValues.TYPE_POLYGON).length; i < len; i++) {
            ClickHouseGeoRingValue.check(value[i]);
        }

        return value;
    }

    protected static String convert(double[][][] value) {
        StringBuilder builder = new StringBuilder().append('[');
        for (int i = 0, len = value.length; i < len; i++) {
            builder.append(ClickHouseGeoRingValue.convert(value[i])).append(',');
        }

        if (builder.length() > 1) {
            builder.setLength(builder.length() - 1);
        }
        return builder.append(']').toString();
    }

    protected ClickHouseGeoPolygonValue(double[][][] value) {
        super(value);
    }

    @Override
    protected ClickHouseGeoPolygonValue set(double[][][] value) {
        return (ClickHouseGeoPolygonValue) super.set(check(value));
    }

    @Override
    public ClickHouseGeoPolygonValue copy(boolean deep) {
        if (!deep) {
            return new ClickHouseGeoPolygonValue(getValue());
        }

        double[][][] value = getValue();
        double[][][] newValue = new double[value.length][][];
        int index = 0;
        for (double[][] v : value) {
            double[][] nv = new double[v.length][];
            int i = 0;
            for (double[] d : v) {
                nv[i++] = Arrays.copyOf(d, d.length);
            }
            newValue[index++] = nv;
        }
        return new ClickHouseGeoPolygonValue(newValue);
    }

    @Override
    public Object[] asArray() {
        return getValue();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T[] asArray(Class<T> clazz) {
        double[][][] v = getValue();
        T[] array = (T[]) Array.newInstance(ClickHouseChecker.nonNull(clazz, ClickHouseValues.TYPE_CLASS), v.length);
        int index = 0;
        for (double[][] d : v) {
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
        for (double[][] d : getValue()) {
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
        return getValue().length == 0;
    }

    @Override
    public ClickHouseGeoPolygonValue resetToDefault() {
        set(EMPTY_VALUE);
        return this;
    }

    @Override
    public ClickHouseGeoPolygonValue resetToNullOrEmpty() {
        return resetToDefault();
    }

    @Override
    public String toSqlExpression() {
        return convert(getValue());
    }

    @Override
    public ClickHouseGeoPolygonValue update(boolean value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_BOOLEAN, ClickHouseValues.TYPE_POLYGON);
    }

    @Override
    public ClickHouseGeoPolygonValue update(boolean[] value) {
        if (value == null || value.length != 2) {
            throw new IllegalArgumentException(ClickHouseValues.ERROR_INVALID_POINT + Arrays.toString(value));
        }
        set(new double[][][] { new double[][] { new double[] { value[0] ? 1 : 0, value[1] ? 0 : 1 } } });
        return this;
    }

    @Override
    public ClickHouseGeoPolygonValue update(char value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_CHAR, ClickHouseValues.TYPE_POLYGON);
    }

    @Override
    public ClickHouseGeoPolygonValue update(char[] value) {
        if (value == null || value.length != 2) {
            throw new IllegalArgumentException(ClickHouseValues.ERROR_INVALID_POINT + Arrays.toString(value));
        }
        set(new double[][][] { new double[][] { new double[] { value[0], value[1] } } });
        return this;
    }

    @Override
    public ClickHouseGeoPolygonValue update(byte value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_BYTE, ClickHouseValues.TYPE_POLYGON);
    }

    @Override
    public ClickHouseGeoPolygonValue update(byte[] value) {
        if (value == null || value.length != 2) {
            throw new IllegalArgumentException(ClickHouseValues.ERROR_INVALID_POINT + Arrays.toString(value));
        }
        set(new double[][][] { new double[][] { new double[] { value[0], value[1] } } });
        return this;
    }

    @Override
    public ClickHouseGeoPolygonValue update(short value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_SHORT, ClickHouseValues.TYPE_POLYGON);
    }

    @Override
    public ClickHouseGeoPolygonValue update(short[] value) {
        if (value == null || value.length != 2) {
            throw new IllegalArgumentException(ClickHouseValues.ERROR_INVALID_POINT + Arrays.toString(value));
        }
        set(new double[][][] { new double[][] { new double[] { value[0], value[1] } } });
        return this;
    }

    @Override
    public ClickHouseGeoPolygonValue update(int value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_INT, ClickHouseValues.TYPE_POLYGON);
    }

    @Override
    public ClickHouseGeoPolygonValue update(int[] value) {
        if (value == null || value.length != 2) {
            throw new IllegalArgumentException(ClickHouseValues.ERROR_INVALID_POINT + Arrays.toString(value));
        }
        set(new double[][][] { new double[][] { new double[] { value[0], value[1] } } });
        return this;
    }

    @Override
    public ClickHouseGeoPolygonValue update(long value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_LONG, ClickHouseValues.TYPE_POLYGON);
    }

    @Override
    public ClickHouseGeoPolygonValue update(long[] value) {
        if (value == null || value.length != 2) {
            throw new IllegalArgumentException(ClickHouseValues.ERROR_INVALID_POINT + Arrays.toString(value));
        }
        set(new double[][][] { new double[][] { new double[] { value[0], value[1] } } });
        return this;
    }

    @Override
    public ClickHouseGeoPolygonValue update(float value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_FLOAT, ClickHouseValues.TYPE_POLYGON);
    }

    @Override
    public ClickHouseGeoPolygonValue update(float[] value) {
        if (value == null || value.length != 2) {
            throw new IllegalArgumentException(ClickHouseValues.ERROR_INVALID_POINT + Arrays.toString(value));
        }
        set(new double[][][] { new double[][] { new double[] { value[0], value[1] } } });
        return this;
    }

    @Override
    public ClickHouseGeoPolygonValue update(double value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_DOUBLE, ClickHouseValues.TYPE_POLYGON);
    }

    @Override
    public ClickHouseGeoPolygonValue update(double[] value) {
        if (value == null || value.length != 2) {
            throw new IllegalArgumentException(ClickHouseValues.ERROR_INVALID_POINT + Arrays.toString(value));
        }
        set(new double[][][] { new double[][] { value } });
        return this;
    }

    @Override
    public ClickHouseGeoPolygonValue update(BigInteger value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_BIG_INTEGER, ClickHouseValues.TYPE_POLYGON);
    }

    @Override
    public ClickHouseGeoPolygonValue update(BigDecimal value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_BIG_DECIMAL, ClickHouseValues.TYPE_POLYGON);
    }

    @Override
    public ClickHouseGeoPolygonValue update(Enum<?> value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_ENUM, ClickHouseValues.TYPE_POLYGON);
    }

    @Override
    public ClickHouseGeoPolygonValue update(Inet4Address value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_IPV4, ClickHouseValues.TYPE_POLYGON);
    }

    @Override
    public ClickHouseGeoPolygonValue update(Inet6Address value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_IPV6, ClickHouseValues.TYPE_POLYGON);
    }

    @Override
    public ClickHouseGeoPolygonValue update(LocalDate value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_DATE, ClickHouseValues.TYPE_POLYGON);
    }

    @Override
    public ClickHouseGeoPolygonValue update(LocalTime value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_TIME, ClickHouseValues.TYPE_POLYGON);
    }

    @Override
    public ClickHouseGeoPolygonValue update(LocalDateTime value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_DATE_TIME, ClickHouseValues.TYPE_POLYGON);
    }

    @Override
    public ClickHouseGeoPolygonValue update(Collection<?> value) {
        if (value == null || value.size() != 2) {
            throw new IllegalArgumentException(ClickHouseValues.ERROR_INVALID_POINT + value);
        }
        Iterator<?> i = value.iterator();
        Object v1 = i.next();
        Object v2 = i.next();
        if (v1 instanceof Number) {
            set(new double[][][] {
                    new double[][] { new double[] { ((Number) v1).doubleValue(), ((Number) v2).doubleValue() } } });
        } else {
            set(new double[][][] { new double[][] {
                    new double[] { Double.parseDouble(v1.toString()), Double.parseDouble(v2.toString()) } } });
        }
        return this;
    }

    @Override
    public ClickHouseGeoPolygonValue update(Enumeration<?> value) {
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
            set(new double[][][] {
                    new double[][] { new double[] { ((Number) v1).doubleValue(), ((Number) v2).doubleValue() } } });
        } else {
            set(new double[][][] { new double[][] {
                    new double[] { Double.parseDouble(v1.toString()), Double.parseDouble(v2.toString()) } } });
        }
        return this;
    }

    @Override
    public ClickHouseGeoPolygonValue update(Map<?, ?> value) {
        if (value == null || value.size() != 2) {
            throw new IllegalArgumentException(ClickHouseValues.ERROR_INVALID_POINT + value);
        }
        Iterator<?> i = value.values().iterator();
        Object v1 = i.next();
        Object v2 = i.next();
        if (v1 instanceof Number) {
            set(new double[][][] {
                    new double[][] { new double[] { ((Number) v1).doubleValue(), ((Number) v2).doubleValue() } } });
        } else {
            set(new double[][][] { new double[][] {
                    new double[] { Double.parseDouble(v1.toString()), Double.parseDouble(v2.toString()) } } });
        }
        return this;
    }

    @Override
    public ClickHouseGeoPolygonValue update(String value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_STRING, ClickHouseValues.TYPE_POLYGON);
    }

    @Override
    public ClickHouseGeoPolygonValue update(UUID value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_UUID, ClickHouseValues.TYPE_POLYGON);
    }

    @Override
    public ClickHouseGeoPolygonValue update(ClickHouseValue value) {
        if (value == null || value.isNullOrEmpty()) {
            resetToNullOrEmpty();
        } else if (value instanceof ClickHouseGeoPolygonValue) {
            set(((ClickHouseGeoPolygonValue) value).getValue());
        } else {
            update(value.asArray());
        }
        return this;
    }

    @Override
    public ClickHouseGeoPolygonValue update(Object[] value) {
        if (value instanceof double[][][]) {
            return set((double[][][]) value);
        } else if (value == null || value.length != 2) {
            throw new IllegalArgumentException(ClickHouseValues.ERROR_INVALID_POINT + Arrays.toString(value));
        }
        Object v1 = value[0];
        Object v2 = value[1];
        if (v1 instanceof Number) {
            set(new double[][][] {
                    new double[][] { new double[] { ((Number) v1).doubleValue(), ((Number) v2).doubleValue() } } });
        } else {
            set(new double[][][] { new double[][] {
                    new double[] { Double.parseDouble(v1.toString()), Double.parseDouble(v2.toString()) } } });
        }
        return this;
    }

    @Override
    public ClickHouseGeoPolygonValue update(Object value) {
        if (value instanceof double[][][]) {
            set((double[][][]) value);
        } else {
            super.update(value);
        }
        return this;
    }
}

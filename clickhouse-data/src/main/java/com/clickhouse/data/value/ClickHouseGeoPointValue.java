package com.clickhouse.data.value;

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
import java.util.Map;
import java.util.UUID;
import com.clickhouse.data.ClickHouseValue;
import com.clickhouse.data.ClickHouseValues;

/**
 * Wrapper class of {@code Point}.
 */
public class ClickHouseGeoPointValue extends ClickHouseObjectValue<double[]> {
    /**
     * Creats a point of origin.
     *
     * @return point of origin
     */
    public static ClickHouseGeoPointValue ofOrigin() {
        return of(null, new double[] { 0D, 0D });
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ClickHouseGeoPointValue of(double[] value) {
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
    public static ClickHouseGeoPointValue of(ClickHouseValue ref, double[] value) {
        return ref instanceof ClickHouseGeoPointValue ? ((ClickHouseGeoPointValue) ref).set(value)
                : new ClickHouseGeoPointValue(value);
    }

    protected static double[] check(double[] value) {
        if (value == null || value.length != 2) {
            throw new IllegalArgumentException("Non-null X and Y coordinates are required");
        }

        return value;
    }

    protected static String convert(double[] value) {
        return new StringBuilder().append('(').append(value[0]).append(',').append(value[1]).append(')')
                .toString();
    }

    protected ClickHouseGeoPointValue(double[] value) {
        super(value);
    }

    @Override
    protected ClickHouseGeoPointValue set(double[] value) {
        super.set(check(value));
        return this;
    }

    @Override
    public ClickHouseGeoPointValue copy(boolean deep) {
        if (!deep) {
            return new ClickHouseGeoPointValue(getValue());
        }

        double[] value = getValue();
        double[] newValue = new double[value.length];
        System.arraycopy(value, 0, newValue, 0, value.length);
        return new ClickHouseGeoPointValue(newValue);
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
        return false;
    }

    @Override
    public ClickHouseGeoPointValue resetToDefault() {
        set(new double[] { 0D, 0D });
        return this;
    }

    @Override
    public ClickHouseGeoPointValue resetToNullOrEmpty() {
        return resetToDefault();
    }

    @Override
    public String toSqlExpression() {
        return convert(getValue());
    }

    @Override
    public ClickHouseGeoPointValue update(boolean value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_BOOLEAN, ClickHouseValues.TYPE_POINT);
    }

    @Override
    public ClickHouseGeoPointValue update(boolean[] value) {
        if (value == null || value.length != 2) {
            throw new IllegalArgumentException(ClickHouseValues.ERROR_INVALID_POINT + Arrays.toString(value));
        }
        set(new double[] { value[0] ? 1 : 0, value[1] ? 0 : 1 });
        return this;
    }

    @Override
    public ClickHouseGeoPointValue update(char value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_CHAR, ClickHouseValues.TYPE_POINT);
    }

    @Override
    public ClickHouseGeoPointValue update(char[] value) {
        if (value == null || value.length != 2) {
            throw new IllegalArgumentException(ClickHouseValues.ERROR_INVALID_POINT + Arrays.toString(value));
        }
        set(new double[] { value[0], value[1] });
        return this;
    }

    @Override
    public ClickHouseGeoPointValue update(byte value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_BYTE, ClickHouseValues.TYPE_POINT);
    }

    @Override
    public ClickHouseGeoPointValue update(byte[] value) {
        if (value == null || value.length != 2) {
            throw new IllegalArgumentException(ClickHouseValues.ERROR_INVALID_POINT + Arrays.toString(value));
        }
        set(new double[] { value[0], value[1] });
        return this;
    }

    @Override
    public ClickHouseGeoPointValue update(short value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_SHORT, ClickHouseValues.TYPE_POINT);
    }

    @Override
    public ClickHouseGeoPointValue update(short[] value) {
        if (value == null || value.length != 2) {
            throw new IllegalArgumentException(ClickHouseValues.ERROR_INVALID_POINT + Arrays.toString(value));
        }
        set(new double[] { value[0], value[1] });
        return this;
    }

    @Override
    public ClickHouseGeoPointValue update(int value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_INT, ClickHouseValues.TYPE_POINT);
    }

    @Override
    public ClickHouseGeoPointValue update(int[] value) {
        if (value == null || value.length != 2) {
            throw new IllegalArgumentException(ClickHouseValues.ERROR_INVALID_POINT + Arrays.toString(value));
        }
        set(new double[] { value[0], value[1] });
        return this;
    }

    @Override
    public ClickHouseGeoPointValue update(long value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_LONG, ClickHouseValues.TYPE_POINT);
    }

    @Override
    public ClickHouseGeoPointValue update(long[] value) {
        if (value == null || value.length != 2) {
            throw new IllegalArgumentException(ClickHouseValues.ERROR_INVALID_POINT + Arrays.toString(value));
        }
        set(new double[] { value[0], value[1] });
        return this;
    }

    @Override
    public ClickHouseGeoPointValue update(float value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_FLOAT, ClickHouseValues.TYPE_POINT);
    }

    @Override
    public ClickHouseGeoPointValue update(float[] value) {
        if (value == null || value.length != 2) {
            throw new IllegalArgumentException(ClickHouseValues.ERROR_INVALID_POINT + Arrays.toString(value));
        }
        set(new double[] { value[0], value[1] });
        return this;
    }

    @Override
    public ClickHouseGeoPointValue update(double value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_DOUBLE, ClickHouseValues.TYPE_POINT);
    }

    @Override
    public ClickHouseGeoPointValue update(double[] value) {
        if (value == null || value.length != 2) {
            throw new IllegalArgumentException(ClickHouseValues.ERROR_INVALID_POINT + Arrays.toString(value));
        }
        set(value);
        return this;
    }

    @Override
    public ClickHouseGeoPointValue update(BigInteger value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_BIG_INTEGER, ClickHouseValues.TYPE_POINT);
    }

    @Override
    public ClickHouseGeoPointValue update(BigDecimal value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_BIG_DECIMAL, ClickHouseValues.TYPE_POINT);
    }

    @Override
    public ClickHouseGeoPointValue update(Enum<?> value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_ENUM, ClickHouseValues.TYPE_POINT);
    }

    @Override
    public ClickHouseGeoPointValue update(Inet4Address value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_IPV4, ClickHouseValues.TYPE_POINT);
    }

    @Override
    public ClickHouseGeoPointValue update(Inet6Address value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_IPV6, ClickHouseValues.TYPE_POINT);
    }

    @Override
    public ClickHouseGeoPointValue update(LocalDate value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_DATE, ClickHouseValues.TYPE_POINT);
    }

    @Override
    public ClickHouseGeoPointValue update(LocalTime value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_TIME, ClickHouseValues.TYPE_POINT);
    }

    @Override
    public ClickHouseGeoPointValue update(LocalDateTime value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_DATE_TIME, ClickHouseValues.TYPE_POINT);
    }

    @Override
    public ClickHouseGeoPointValue update(Collection<?> value) {
        if (value == null || value.size() != 2) {
            throw new IllegalArgumentException(ClickHouseValues.ERROR_INVALID_POINT + value);
        }
        Iterator<?> i = value.iterator();
        Object v1 = i.next();
        Object v2 = i.next();
        if (v1 instanceof Number) {
            set(new double[] { ((Number) v1).doubleValue(), ((Number) v2).doubleValue() });
        } else {
            set(new double[] { Double.parseDouble(v1.toString()), Double.parseDouble(v2.toString()) });
        }
        return this;
    }

    @Override
    public ClickHouseGeoPointValue update(Enumeration<?> value) {
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
            set(new double[] { ((Number) v1).doubleValue(), ((Number) v2).doubleValue() });
        } else {
            set(new double[] { Double.parseDouble(v1.toString()), Double.parseDouble(v2.toString()) });
        }
        return this;
    }

    @Override
    public ClickHouseGeoPointValue update(Map<?, ?> value) {
        if (value == null || value.size() != 2) {
            throw new IllegalArgumentException(ClickHouseValues.ERROR_INVALID_POINT + value);
        }
        Iterator<?> i = value.values().iterator();
        Object v1 = i.next();
        Object v2 = i.next();
        if (v1 instanceof Number) {
            set(new double[] { ((Number) v1).doubleValue(), ((Number) v2).doubleValue() });
        } else {
            set(new double[] { Double.parseDouble(v1.toString()), Double.parseDouble(v2.toString()) });
        }
        return this;
    }

    @Override
    public ClickHouseGeoPointValue update(String value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_STRING, ClickHouseValues.TYPE_POINT);
    }

    @Override
    public ClickHouseGeoPointValue update(UUID value) {
        throw newUnsupportedException(ClickHouseValues.TYPE_UUID, ClickHouseValues.TYPE_POINT);
    }

    @Override
    public ClickHouseGeoPointValue update(ClickHouseValue value) {
        if (value == null || value.isNullOrEmpty()) {
            resetToNullOrEmpty();
        } else if (value instanceof ClickHouseGeoPointValue) {
            set(((ClickHouseGeoPointValue) value).getValue());
        } else {
            update(value.asArray());
        }
        return this;
    }

    @Override
    public ClickHouseGeoPointValue update(Object[] value) {
        if (value == null || value.length != 2) {
            throw new IllegalArgumentException(ClickHouseValues.ERROR_INVALID_POINT + Arrays.toString(value));
        }
        Object v1 = value[0];
        Object v2 = value[1];
        if (v1 instanceof Number) {
            set(new double[] { ((Number) v1).doubleValue(), ((Number) v2).doubleValue() });
        } else {
            set(new double[] { Double.parseDouble(v1.toString()), Double.parseDouble(v2.toString()) });
        }
        return this;
    }

    @Override
    public ClickHouseGeoPointValue update(Object value) {
        if (value instanceof double[]) {
            update((double[]) value);
        } else {
            super.update(value);
        }
        return this;
    }
}

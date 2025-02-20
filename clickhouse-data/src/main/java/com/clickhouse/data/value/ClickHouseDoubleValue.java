package com.clickhouse.data.value;

import java.math.BigDecimal;
import java.math.BigInteger;

import com.clickhouse.data.ClickHouseDataConfig;
import com.clickhouse.data.ClickHouseValue;
import com.clickhouse.data.ClickHouseValues;

/**
 * Wrapper class of {@code double}.
 */
@Deprecated
public class ClickHouseDoubleValue implements ClickHouseValue {
    /**
     * Create a new instance representing null value.
     *
     * @return new instance representing null value
     */
    public static ClickHouseDoubleValue ofNull() {
        return ofNull(null);
    }

    /**
     * Update given value to null or create a new instance if {@code ref} is
     * null.
     *
     * @param ref object to update, could be null
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ClickHouseDoubleValue ofNull(ClickHouseValue ref) {
        return ref instanceof ClickHouseDoubleValue ? ((ClickHouseDoubleValue) ref).set(true, 0D)
                : new ClickHouseDoubleValue(true, 0D);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ClickHouseDoubleValue of(double value) {
        return of(null, value);
    }

    /**
     * Update value of the given object or create a new instance if {@code
     * ref} is null.
     *
     * @param ref   object to update, could be null
     * @param value value
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ClickHouseDoubleValue of(ClickHouseValue ref, double value) {
        return ref instanceof ClickHouseDoubleValue ? ((ClickHouseDoubleValue) ref).set(false, value)
                : new ClickHouseDoubleValue(false, value);
    }

    private boolean isNull;
    private double value;

    protected ClickHouseDoubleValue(boolean isNull, double value) {
        set(isNull, value);
    }

    protected ClickHouseDoubleValue set(boolean isNull, double value) {
        this.isNull = isNull;
        this.value = isNull ? 0L : value;

        return this;
    }

    /**
     * Gets value.
     *
     * @return value
     */
    public double getValue() {
        return value;
    }

    @Override
    public ClickHouseDoubleValue copy(boolean deep) {
        return new ClickHouseDoubleValue(isNull, value);
    }

    @Override
    public boolean isNullOrEmpty() {
        return isNull;
    }

    @Override
    public byte asByte() {
        return (byte) value;
    }

    @Override
    public short asShort() {
        return (short) value;
    }

    @Override
    public int asInteger() {
        return (int) value;
    }

    @Override
    public long asLong() {
        return (long) value;
    }

    @Override
    public BigInteger asBigInteger() {
        return isNull ? null : BigDecimal.valueOf(value).toBigInteger();
    }

    @Override
    public float asFloat() {
        return (float) value;
    }

    @Override
    public double asDouble() {
        return value;
    }

    @Override
    public BigDecimal asBigDecimal() {
        if (isNull) {
            return null;
        } else if (Double.isNaN(value) || value == Double.POSITIVE_INFINITY || value == Double.NEGATIVE_INFINITY) {
            throw new NumberFormatException(ClickHouseValues.ERROR_INF_OR_NAN);
        } else if (value == 0D) {
            return BigDecimal.ZERO;
        } else if (value == 1D) {
            return BigDecimal.ONE;
        }
        return new BigDecimal(Double.toString(value));
    }

    @Override
    public BigDecimal asBigDecimal(int scale) {
        if (isNull) {
            return null;
        } else if (Double.isNaN(value) || value == Double.POSITIVE_INFINITY || value == Double.NEGATIVE_INFINITY) {
            throw new NumberFormatException(ClickHouseValues.ERROR_INF_OR_NAN);
        }

        BigDecimal dec = BigDecimal.valueOf(value);
        if (value == 0D) {
            dec = dec.setScale(scale);
        } else {
            int diff = scale - dec.scale();
            if (diff > 0) {
                dec = dec.divide(BigDecimal.TEN.pow(diff + 1));
            } else if (diff < 0) {
                dec = dec.setScale(scale, ClickHouseDataConfig.DEFAULT_ROUNDING_MODE);
            }
        }
        return dec;
    }

    @Override
    public Object asObject() {
        return isNull ? null : getValue();
    }

    @Override
    public String asString() {
        if (isNull) {
            return null;
        }

        return String.valueOf(value);
    }

    @Override
    public ClickHouseDoubleValue resetToDefault() {
        return set(false, 0D);
    }

    @Override
    public ClickHouseDoubleValue resetToNullOrEmpty() {
        return set(true, 0D);
    }

    @Override
    public String toSqlExpression() {
        if (isNullOrEmpty()) {
            return ClickHouseValues.NULL_EXPR;
        } else if (isNaN()) {
            return ClickHouseValues.NAN_EXPR;
        } else if (value == Float.POSITIVE_INFINITY) {
            return ClickHouseValues.INF_EXPR;
        } else if (value == Float.NEGATIVE_INFINITY) {
            return ClickHouseValues.NINF_EXPR;
        }
        return String.valueOf(value);
    }

    @Override
    public ClickHouseDoubleValue update(boolean value) {
        return set(false, value ? 1 : 0);
    }

    @Override
    public ClickHouseDoubleValue update(char value) {
        return set(false, value);
    }

    @Override
    public ClickHouseDoubleValue update(byte value) {
        return set(false, value);
    }

    @Override
    public ClickHouseDoubleValue update(short value) {
        return set(false, value);
    }

    @Override
    public ClickHouseDoubleValue update(int value) {
        return set(false, value);
    }

    @Override
    public ClickHouseDoubleValue update(long value) {
        return set(false, value);
    }

    @Override
    public ClickHouseDoubleValue update(float value) {
        return set(false, value);
    }

    @Override
    public ClickHouseDoubleValue update(double value) {
        return set(false, value);
    }

    @Override
    public ClickHouseDoubleValue update(BigInteger value) {
        return value == null ? resetToNullOrEmpty() : set(false, value.doubleValue());
    }

    @Override
    public ClickHouseDoubleValue update(BigDecimal value) {
        return value == null ? resetToNullOrEmpty() : set(false, value.doubleValue());
    }

    @Override
    public ClickHouseDoubleValue update(Enum<?> value) {
        return value == null ? resetToNullOrEmpty() : set(false, value.ordinal());
    }

    @Override
    public ClickHouseDoubleValue update(String value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else if (value.isEmpty()) {
            resetToDefault();
        } else if (value.equals("nan")) {
            set(false, Double.NaN);
        } else if (value.equals("+inf") || value.equals("inf")) {
            set(false, Double.POSITIVE_INFINITY);
        } else if (value.equals("-inf")) {
            set(false, Double.NEGATIVE_INFINITY);
        } else {
            set(false, Double.parseDouble(value));
        }
        return this;
    }

    @Override
    public ClickHouseDoubleValue update(ClickHouseValue value) {
        return value == null || value.isNullOrEmpty() ? resetToNullOrEmpty() : set(false, value.asDouble());
    }

    @Override
    public ClickHouseDoubleValue update(Object value) {
        if (value instanceof Number) {
            return set(false, ((Number) value).doubleValue());
        } else if (value instanceof ClickHouseValue) {
            return set(false, ((ClickHouseValue) value).asDouble());
        }
        ClickHouseValue.super.update(value);
        return this;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { // too bad this is a mutable class :<
            return true;
        } else if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ClickHouseDoubleValue v = (ClickHouseDoubleValue) obj;
        return isNull == v.isNull && value == v.value;
    }

    @Override
    public int hashCode() {
        // not going to use Objects.hash(isNull, value) due to autoboxing
        long l = Double.doubleToLongBits(value);
        return (31 + (isNull ? 1231 : 1237)) * 31 + (int) (l ^ (l >>> 32));
    }

    @Override
    public String toString() {
        return ClickHouseValues.convertToString(this);
    }
}

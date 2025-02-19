package com.clickhouse.data.value;

import java.math.BigDecimal;
import java.math.BigInteger;

import com.clickhouse.data.ClickHouseDataConfig;
import com.clickhouse.data.ClickHouseValue;
import com.clickhouse.data.ClickHouseValues;

/**
 * Wrapper class of {@code float}.
 */
@Deprecated
public class ClickHouseFloatValue implements ClickHouseValue {
    /**
     * Create a new instance representing null value.
     *
     * @return new instance representing null value
     */
    public static ClickHouseFloatValue ofNull() {
        return ofNull(null);
    }

    /**
     * Update given value to null or create a new instance if {@code ref} is null.
     * 
     * @param ref object to update, could be null
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ClickHouseFloatValue ofNull(ClickHouseValue ref) {
        return ref instanceof ClickHouseFloatValue ? ((ClickHouseFloatValue) ref).set(true, 0F)
                : new ClickHouseFloatValue(true, 0F);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ClickHouseFloatValue of(float value) {
        return of(null, value);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ClickHouseFloatValue of(Number value) {
        return value == null ? ofNull(null) : of(null, value.floatValue());
    }

    /**
     * Update value of the given object or create a new instance if {@code ref} is
     * null.
     *
     * @param ref   object to update, could be null
     * @param value value
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ClickHouseFloatValue of(ClickHouseValue ref, float value) {
        return ref instanceof ClickHouseFloatValue ? ((ClickHouseFloatValue) ref).set(false, value)
                : new ClickHouseFloatValue(false, value);
    }

    private boolean isNull;
    private float value;

    protected ClickHouseFloatValue(boolean isNull, float value) {
        set(isNull, value);
    }

    protected ClickHouseFloatValue set(boolean isNull, float value) {
        this.isNull = isNull;
        this.value = isNull ? 0F : value;

        return this;
    }

    /**
     * Gets value.
     *
     * @return value
     */
    public float getValue() {
        return value;
    }

    @Override
    public ClickHouseFloatValue copy(boolean deep) {
        return new ClickHouseFloatValue(isNull, value);
    }

    @Override
    public boolean isInfinity() {
        return value == Float.POSITIVE_INFINITY || value == Float.NEGATIVE_INFINITY;
    }

    @Override
    public boolean isNaN() {
        return Float.isNaN(value);
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
        return value;
    }

    @Override
    public double asDouble() {
        return value;
    }

    @Override
    public BigDecimal asBigDecimal() {
        if (isNull) {
            return null;
        } else if (Float.isNaN(value) || value == Float.POSITIVE_INFINITY || value == Float.NEGATIVE_INFINITY) {
            throw new NumberFormatException(ClickHouseValues.ERROR_INF_OR_NAN);
        } else if (value == 0F) {
            return BigDecimal.ZERO;
        } else if (value == 1F) {
            return BigDecimal.ONE;
        }
        return new BigDecimal(Float.toString(value));
    }

    @Override
    public BigDecimal asBigDecimal(int scale) {
        if (isNull) {
            return null;
        } else if (Float.isNaN(value) || value == Float.POSITIVE_INFINITY || value == Float.NEGATIVE_INFINITY) {
            throw new NumberFormatException(ClickHouseValues.ERROR_INF_OR_NAN);
        }

        BigDecimal dec = new BigDecimal(Float.toString(value));
        if (value == 0F) {
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
    public ClickHouseFloatValue resetToDefault() {
        return set(false, 0F);
    }

    @Override
    public ClickHouseFloatValue resetToNullOrEmpty() {
        return set(true, 0F);
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
    public ClickHouseFloatValue update(boolean value) {
        return set(false, value ? 1F : 0F);
    }

    @Override
    public ClickHouseFloatValue update(char value) {
        return set(false, value);
    }

    @Override
    public ClickHouseFloatValue update(byte value) {
        return set(false, value);
    }

    @Override
    public ClickHouseFloatValue update(short value) {
        return set(false, value);
    }

    @Override
    public ClickHouseFloatValue update(int value) {
        return set(false, value);
    }

    @Override
    public ClickHouseFloatValue update(long value) {
        return set(false, value);
    }

    @Override
    public ClickHouseFloatValue update(float value) {
        return set(false, value);
    }

    @Override
    public ClickHouseFloatValue update(double value) {
        return set(false, (float) value);
    }

    @Override
    public ClickHouseFloatValue update(BigInteger value) {
        return value == null ? resetToNullOrEmpty() : set(false, value.floatValue());
    }

    @Override
    public ClickHouseFloatValue update(BigDecimal value) {
        return value == null ? resetToNullOrEmpty() : set(false, value.floatValue());
    }

    @Override
    public ClickHouseFloatValue update(Enum<?> value) {
        return value == null ? resetToNullOrEmpty() : set(false, value.ordinal());
    }

    @Override
    public ClickHouseFloatValue update(String value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else if (value.isEmpty()) {
            resetToDefault();
        } else {
            set(false, Float.parseFloat(value));
        }
        return this;
    }

    @Override
    public ClickHouseFloatValue update(ClickHouseValue value) {
        return value == null || value.isNullOrEmpty() ? resetToNullOrEmpty() : set(false, value.asFloat());
    }

    @Override
    public ClickHouseFloatValue update(Object value) {
        if (value instanceof Number) {
            return set(false, ((Number) value).floatValue());
        } else if (value instanceof ClickHouseValue) {
            return set(false, ((ClickHouseValue) value).asFloat());
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

        ClickHouseFloatValue v = (ClickHouseFloatValue) obj;
        return isNull == v.isNull && value == v.value;
    }

    @Override
    public int hashCode() {
        // not going to use Objects.hash(isNull, value) due to autoboxing
        return (31 + (isNull ? 1231 : 1237)) * 31 + Float.floatToIntBits(value);
    }

    @Override
    public String toString() {
        return ClickHouseValues.convertToString(this);
    }
}

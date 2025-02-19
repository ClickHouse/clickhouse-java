package com.clickhouse.data.value;

import java.math.BigDecimal;
import java.math.BigInteger;
import com.clickhouse.data.ClickHouseValue;
import com.clickhouse.data.ClickHouseValues;

/**
 * Wrapper class of {@code bool}.
 */
@Deprecated
public class ClickHouseBoolValue implements ClickHouseValue {
    private static final String ERROR_INVALID_NUMBER = "Boolean value can be only 1(true) or 0(false).";

    /**
     * Create a new instance representing null value.
     *
     * @return new instance representing null value
     */
    public static ClickHouseBoolValue ofNull() {
        return ofNull(null);
    }

    /**
     * Update given value to null or create a new instance if {@code ref} is null.
     * 
     * @param ref object to update, could be null
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ClickHouseBoolValue ofNull(ClickHouseValue ref) {
        return ref instanceof ClickHouseBoolValue ? ((ClickHouseBoolValue) ref).set(true, false)
                : new ClickHouseBoolValue(true, false);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ClickHouseBoolValue of(boolean value) {
        return of(null, value);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ClickHouseBoolValue of(int value) {
        if (value == 1) {
            return new ClickHouseBoolValue(false, true);
        } else if (value == 0) {
            return new ClickHouseBoolValue(false, false);
        } else {
            throw new IllegalArgumentException(ERROR_INVALID_NUMBER);
        }
    }

    /**
     * Update value of the given object or create a new instance if {@code ref} is
     * null.
     *
     * @param ref   object to update, could be null
     * @param value value
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ClickHouseBoolValue of(ClickHouseValue ref, boolean value) {
        return ref instanceof ClickHouseBoolValue ? ((ClickHouseBoolValue) ref).set(false, value)
                : new ClickHouseBoolValue(false, value);
    }

    private boolean isNull;
    private boolean value;

    protected ClickHouseBoolValue(boolean isNull, boolean value) {
        set(isNull, value);
    }

    protected ClickHouseBoolValue set(boolean isNull, boolean value) {
        this.isNull = isNull;
        this.value = !isNull && value;

        return this;
    }

    /**
     * Gets value.
     *
     * @return value
     */
    public boolean getValue() {
        return value;
    }

    @Override
    public ClickHouseBoolValue copy(boolean deep) {
        return new ClickHouseBoolValue(isNull, value);
    }

    @Override
    public boolean isNullOrEmpty() {
        return isNull;
    }

    @Override
    public byte asByte() {
        return value ? (byte) 1 : (byte) 0;
    }

    @Override
    public short asShort() {
        return value ? (short) 1 : (short) 0;
    }

    @Override
    public int asInteger() {
        return value ? 1 : 0;
    }

    @Override
    public long asLong() {
        return value ? 1L : 0L;
    }

    @Override
    public BigInteger asBigInteger() {
        if (isNull) {
            return null;
        }

        return value ? BigInteger.ONE : BigInteger.ZERO;
    }

    @Override
    public float asFloat() {
        return value ? 1F : 0F;
    }

    @Override
    public double asDouble() {
        return value ? 1D : 0D;
    }

    @Override
    public BigDecimal asBigDecimal(int scale) {
        if (isNull) {
            return null;
        }

        return BigDecimal.valueOf(value ? 1L : 0L, scale);
    }

    @Override
    public Object asObject() {
        return isNull ? null : Boolean.valueOf(value);
    }

    @Override
    public String asString() {
        if (isNull) {
            return null;
        }

        return String.valueOf(value);
    }

    @Override
    public ClickHouseBoolValue resetToDefault() {
        return set(false, false);
    }

    @Override
    public ClickHouseBoolValue resetToNullOrEmpty() {
        return set(true, false);
    }

    @Override
    public String toSqlExpression() {
        if (isNull) {
            return ClickHouseValues.NULL_EXPR;
        }

        // prefer to use number format to ensure backward compatibility
        return value ? "1" : "0";
    }

    @Override
    public ClickHouseBoolValue update(char value) {
        return set(false, ClickHouseValues.convertToBoolean(value));
    }

    @Override
    public ClickHouseBoolValue update(byte value) {
        if (value == (byte) 1) {
            return set(false, true);
        } else if (value == (byte) 0) {
            return set(false, false);
        } else {
            throw new IllegalArgumentException(ERROR_INVALID_NUMBER);
        }
    }

    @Override
    public ClickHouseBoolValue update(short value) {
        if (value == (short) 1) {
            return set(false, true);
        } else if (value == (short) 0) {
            return set(false, false);
        } else {
            throw new IllegalArgumentException(ERROR_INVALID_NUMBER);
        }
    }

    @Override
    public ClickHouseBoolValue update(int value) {
        if (value == 1) {
            return set(false, true);
        } else if (value == 0) {
            return set(false, false);
        } else {
            throw new IllegalArgumentException(ERROR_INVALID_NUMBER);
        }
    }

    @Override
    public ClickHouseBoolValue update(long value) {
        if (value == 1L) {
            return set(false, true);
        } else if (value == 0L) {
            return set(false, false);
        } else {
            throw new IllegalArgumentException(ERROR_INVALID_NUMBER);
        }
    }

    @Override
    public ClickHouseBoolValue update(float value) {
        if (value == 1F) {
            return set(false, true);
        } else if (value == 0F) {
            return set(false, false);
        } else {
            throw new IllegalArgumentException(ERROR_INVALID_NUMBER);
        }
    }

    @Override
    public ClickHouseBoolValue update(double value) {
        if (value == 1D) {
            return set(false, true);
        } else if (value == 0D) {
            return set(false, false);
        } else {
            throw new IllegalArgumentException(ERROR_INVALID_NUMBER);
        }
    }

    @Override
    public ClickHouseBoolValue update(BigInteger value) {
        if (value == null) {
            return resetToNullOrEmpty();
        } else if (BigInteger.ONE.equals(value)) {
            return set(false, true);
        } else if (BigInteger.ZERO.equals(value)) {
            return set(false, false);
        } else {
            throw new IllegalArgumentException(ERROR_INVALID_NUMBER);
        }
    }

    @Override
    public ClickHouseBoolValue update(BigDecimal value) {
        if (value == null) {
            return resetToNullOrEmpty();
        } else if (BigDecimal.valueOf(1L, value.scale()).equals(value)) {
            return set(false, true);
        } else if (BigDecimal.valueOf(0L, value.scale()).equals(value)) {
            return set(false, false);
        } else {
            throw new IllegalArgumentException(ERROR_INVALID_NUMBER);
        }
    }

    @Override
    public ClickHouseBoolValue update(Enum<?> value) {
        return value == null ? resetToNullOrEmpty() : update(value.ordinal());
    }

    @Override
    public ClickHouseBoolValue update(String value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        return set(false, ClickHouseValues.convertToBoolean(value));
    }

    @Override
    public ClickHouseBoolValue update(ClickHouseValue value) {
        return value == null || value.isNullOrEmpty() ? resetToNullOrEmpty() : set(false, value.asBoolean());
    }

    @Override
    public ClickHouseBoolValue update(Object value) {
        if (value instanceof Boolean) {
            return set(false, (boolean) value);
        } else if (value instanceof Number) {
            return update(((Number) value).byteValue());
        } else if (value instanceof ClickHouseValue) {
            return set(false, ((ClickHouseValue) value).asBoolean());
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

        ClickHouseBoolValue v = (ClickHouseBoolValue) obj;
        return isNull == v.isNull && value == v.value;
    }

    @Override
    public int hashCode() {
        // not going to use Objects.hash(isNull, value) due to autoboxing
        return (31 + (isNull ? 1231 : 1237)) * 31 + (value ? 1231 : 1237);
    }

    @Override
    public String toString() {
        return ClickHouseValues.convertToString(this);
    }
}

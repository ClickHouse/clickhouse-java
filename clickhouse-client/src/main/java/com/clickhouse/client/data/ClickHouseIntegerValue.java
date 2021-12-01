package com.clickhouse.client.data;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import com.clickhouse.client.ClickHouseChecker;
import com.clickhouse.client.ClickHouseValue;
import com.clickhouse.client.ClickHouseValues;

/**
 * Wraper class of int.
 */
public class ClickHouseIntegerValue implements ClickHouseValue {
    /**
     * Create a new instance representing null value.
     *
     * @return new instance representing null value
     */
    public static ClickHouseIntegerValue ofNull() {
        return ofNull(null);
    }

    /**
     * Update given value to null or create a new instance if {@code ref} is null.
     * 
     * @param ref object to update, could be null
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ClickHouseIntegerValue ofNull(ClickHouseValue ref) {
        return ref instanceof ClickHouseIntegerValue ? ((ClickHouseIntegerValue) ref).set(true, 0)
                : new ClickHouseIntegerValue(true, 0);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ClickHouseIntegerValue of(int value) {
        return of(null, value);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ClickHouseIntegerValue of(Number value) {
        return value == null ? ofNull(null) : of(null, value.intValue());
    }

    /**
     * Update value of the given object or create a new instance if {@code ref} is
     * null.
     *
     * @param ref   object to update, could be null
     * @param value value
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ClickHouseIntegerValue of(ClickHouseValue ref, int value) {
        return ref instanceof ClickHouseIntegerValue ? ((ClickHouseIntegerValue) ref).set(false, value)
                : new ClickHouseIntegerValue(false, value);
    }

    private boolean isNull;
    private int value;

    protected ClickHouseIntegerValue(boolean isNull, int value) {
        set(isNull, value);
    }

    protected ClickHouseIntegerValue set(boolean isNull, int value) {
        this.isNull = isNull;
        this.value = isNull ? 0 : value;

        return this;
    }

    /**
     * Gets value.
     *
     * @return value
     */
    public int getValue() {
        return value;
    }

    @Override
    public ClickHouseIntegerValue copy(boolean deep) {
        return new ClickHouseIntegerValue(isNull, value);
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
        return value;
    }

    @Override
    public long asLong() {
        return value;
    }

    @Override
    public BigInteger asBigInteger() {
        return isNull ? null : BigInteger.valueOf(value);
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
    public BigDecimal asBigDecimal(int scale) {
        return isNull ? null : BigDecimal.valueOf(value, scale);
    }

    @Override
    public Object asObject() {
        return isNull ? null : getValue();
    }

    @Override
    public String asString(int length, Charset charset) {
        if (isNull) {
            return null;
        }

        String str = String.valueOf(value);
        if (length > 0) {
            ClickHouseChecker.notWithDifferentLength(str.getBytes(charset == null ? StandardCharsets.UTF_8 : charset),
                    length);
        }

        return str;
    }

    @Override
    public ClickHouseIntegerValue resetToNullOrEmpty() {
        return set(true, 0);
    }

    @Override
    public String toSqlExpression() {
        return isNullOrEmpty() ? ClickHouseValues.NULL_EXPR : String.valueOf(value);
    }

    @Override
    public ClickHouseIntegerValue update(boolean value) {
        return set(false, value ? 1 : 0);
    }

    @Override
    public ClickHouseIntegerValue update(char value) {
        return set(false, value);
    }

    @Override
    public ClickHouseIntegerValue update(byte value) {
        return set(false, value);
    }

    @Override
    public ClickHouseIntegerValue update(short value) {
        return set(false, value);
    }

    @Override
    public ClickHouseIntegerValue update(int value) {
        return set(false, value);
    }

    @Override
    public ClickHouseIntegerValue update(long value) {
        return set(false, (int) value);
    }

    @Override
    public ClickHouseIntegerValue update(float value) {
        return set(false, (int) value);
    }

    @Override
    public ClickHouseIntegerValue update(double value) {
        return set(false, (int) value);
    }

    @Override
    public ClickHouseIntegerValue update(BigInteger value) {
        return value == null ? resetToNullOrEmpty() : set(false, value.intValueExact());
    }

    @Override
    public ClickHouseIntegerValue update(BigDecimal value) {
        return value == null ? resetToNullOrEmpty() : set(false, value.intValueExact());
    }

    @Override
    public ClickHouseIntegerValue update(Enum<?> value) {
        return value == null ? resetToNullOrEmpty() : set(false, value.ordinal());
    }

    @Override
    public ClickHouseIntegerValue update(String value) {
        return value == null ? resetToNullOrEmpty() : set(false, Integer.parseInt(value));
    }

    @Override
    public ClickHouseIntegerValue update(ClickHouseValue value) {
        return value == null ? resetToNullOrEmpty() : set(false, value.asInteger());
    }

    @Override
    public ClickHouseIntegerValue update(Object value) {
        if (value instanceof Number) {
            return set(false, ((Number) value).intValue());
        } else if (value instanceof ClickHouseValue) {
            return set(false, ((ClickHouseValue) value).asInteger());
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

        ClickHouseIntegerValue v = (ClickHouseIntegerValue) obj;
        return isNull == v.isNull && value == v.value;
    }

    @Override
    public int hashCode() {
        // not going to use Objects.hash(isNull, value) due to autoboxing
        return (31 + (isNull ? 1231 : 1237)) * 31 + value;
    }

    @Override
    public String toString() {
        return ClickHouseValues.convertToString(this);
    }
}

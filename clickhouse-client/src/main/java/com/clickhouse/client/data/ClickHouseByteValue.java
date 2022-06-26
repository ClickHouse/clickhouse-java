package com.clickhouse.client.data;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import com.clickhouse.client.ClickHouseChecker;
import com.clickhouse.client.ClickHouseValue;
import com.clickhouse.client.ClickHouseValues;

/**
 * Wraper class of byte.
 */
public class ClickHouseByteValue implements ClickHouseValue {
    /**
     * Create a new instance representing null value.
     *
     * @return new instance representing null value
     */
    public static ClickHouseByteValue ofNull() {
        return ofNull(null);
    }

    /**
     * Update given value to null or create a new instance if {@code ref} is null.
     * 
     * @param ref object to update, could be null
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ClickHouseByteValue ofNull(ClickHouseValue ref) {
        return ref instanceof ClickHouseByteValue ? ((ClickHouseByteValue) ref).set(true, (byte) 0)
                : new ClickHouseByteValue(true, (byte) 0);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ClickHouseByteValue of(byte value) {
        return of(null, value);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ClickHouseByteValue of(int value) {
        return of(null, (byte) value);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ClickHouseByteValue of(Number value) {
        return value == null ? ofNull(null) : of(null, value.byteValue());
    }

    /**
     * Update value of the given object or create a new instance if {@code ref} is
     * null.
     *
     * @param ref   object to update, could be null
     * @param value value
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ClickHouseByteValue of(ClickHouseValue ref, byte value) {
        return ref instanceof ClickHouseByteValue ? ((ClickHouseByteValue) ref).set(false, value)
                : new ClickHouseByteValue(false, value);
    }

    private boolean isNull;
    private byte value;

    protected ClickHouseByteValue(boolean isNull, byte value) {
        set(isNull, value);
    }

    protected ClickHouseByteValue set(boolean isNull, byte value) {
        this.isNull = isNull;
        this.value = isNull ? (byte) 0 : value;

        return this;
    }

    /**
     * Gets value.
     *
     * @return value
     */
    public byte getValue() {
        return value;
    }

    @Override
    public ClickHouseByteValue copy(boolean deep) {
        return new ClickHouseByteValue(isNull, value);
    }

    @Override
    public boolean isNullOrEmpty() {
        return isNull;
    }

    @Override
    public byte asByte() {
        return value;
    }

    @Override
    public short asShort() {
        return value;
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
        return isNull ? null : Byte.valueOf(value);
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
    public ClickHouseByteValue resetToDefault() {
        return set(false, (byte) 0);
    }

    @Override
    public ClickHouseByteValue resetToNullOrEmpty() {
        return set(true, (byte) 0);
    }

    @Override
    public String toSqlExpression() {
        return isNull ? ClickHouseValues.NULL_EXPR : String.valueOf(value);
    }

    @Override
    public ClickHouseByteValue update(char value) {
        return set(false, (byte) value);
    }

    @Override
    public ClickHouseByteValue update(byte value) {
        return set(false, value);
    }

    @Override
    public ClickHouseByteValue update(short value) {
        return set(false, (byte) value);
    }

    @Override
    public ClickHouseByteValue update(int value) {
        return set(false, (byte) value);
    }

    @Override
    public ClickHouseByteValue update(long value) {
        return set(false, (byte) value);
    }

    @Override
    public ClickHouseByteValue update(float value) {
        return set(false, (byte) value);
    }

    @Override
    public ClickHouseByteValue update(double value) {
        return set(false, (byte) value);
    }

    @Override
    public ClickHouseByteValue update(BigInteger value) {
        return value == null ? resetToNullOrEmpty() : set(false, value.byteValueExact());
    }

    @Override
    public ClickHouseByteValue update(BigDecimal value) {
        return value == null ? resetToNullOrEmpty() : set(false, value.byteValueExact());
    }

    @Override
    public ClickHouseByteValue update(Enum<?> value) {
        return value == null ? resetToNullOrEmpty() : set(false, (byte) value.ordinal());
    }

    @Override
    public ClickHouseByteValue update(String value) {
        return value == null ? resetToNullOrEmpty() : set(false, Byte.parseByte(value));
    }

    @Override
    public ClickHouseByteValue update(ClickHouseValue value) {
        return value == null ? resetToNullOrEmpty() : set(false, value.asByte());
    }

    @Override
    public ClickHouseByteValue update(Object value) {
        if (value instanceof Number) {
            return set(false, ((Number) value).byteValue());
        } else if (value instanceof ClickHouseValue) {
            return set(false, ((ClickHouseValue) value).asByte());
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

        ClickHouseByteValue v = (ClickHouseByteValue) obj;
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

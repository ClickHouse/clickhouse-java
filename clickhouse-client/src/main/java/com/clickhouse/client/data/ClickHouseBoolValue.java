package com.clickhouse.client.data;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import com.clickhouse.client.ClickHouseChecker;
import com.clickhouse.client.ClickHouseValue;
import com.clickhouse.client.ClickHouseValues;

/**
 * Wraper class of bool.
 */
public class ClickHouseBoolValue implements ClickHouseValue {
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
        return of(null, value == 1);
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
        return isNull ? null : (value ? BigInteger.ONE : BigInteger.ZERO);
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
        return isNull ? null : (value ? BigDecimal.ONE : BigDecimal.ZERO);
    }

    @Override
    public Object asObject() {
        return isNull ? null : Boolean.valueOf(value);
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
    public ClickHouseBoolValue resetToNullOrEmpty() {
        return set(true, false);
    }

    @Override
    public String toSqlExpression() {
        return isNull ? ClickHouseValues.NULL_EXPR : String.valueOf(value ? 1 : 0);
    }

    @Override
    public ClickHouseBoolValue update(char value) {
        return set(false, value == 1);
    }

    @Override
    public ClickHouseBoolValue update(byte value) {
        return set(false, value == (byte) 1);
    }

    @Override
    public ClickHouseBoolValue update(short value) {
        return set(false, value == (short) 1);
    }

    @Override
    public ClickHouseBoolValue update(int value) {
        return set(false, value == 1);
    }

    @Override
    public ClickHouseBoolValue update(long value) {
        return set(false, value == 1L);
    }

    @Override
    public ClickHouseBoolValue update(float value) {
        return set(false, value == 1F);
    }

    @Override
    public ClickHouseBoolValue update(double value) {
        return set(false, value == 1D);
    }

    @Override
    public ClickHouseBoolValue update(BigInteger value) {
        return value == null ? resetToNullOrEmpty() : set(false, BigInteger.ONE.equals(value));
    }

    @Override
    public ClickHouseBoolValue update(BigDecimal value) {
        return value == null ? resetToNullOrEmpty() : set(false, BigDecimal.ONE.equals(value));
    }

    @Override
    public ClickHouseBoolValue update(Enum<?> value) {
        return value == null ? resetToNullOrEmpty() : set(false, value.ordinal() == 1);
    }

    @Override
    public ClickHouseBoolValue update(String value) {
        return value == null ? resetToNullOrEmpty() : set(false, Boolean.parseBoolean(value) || "1".equals(value));
    }

    @Override
    public ClickHouseBoolValue update(ClickHouseValue value) {
        return value == null ? resetToNullOrEmpty() : set(false, value.asBoolean());
    }

    @Override
    public ClickHouseBoolValue update(Object value) {
        if (value instanceof Boolean) {
            return set(false, (boolean) value);
        } else if (value instanceof Number) {
            return set(false, ((Number) value).byteValue() == (byte) 0);
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

package com.clickhouse.data.value;

import java.math.BigDecimal;
import java.math.BigInteger;

import com.clickhouse.data.ClickHouseValue;
import com.clickhouse.data.ClickHouseValues;

/**
 * Wrapper class of {@code byte}.
 */
@Deprecated
public class ClickHouseByteValue implements ClickHouseValue {
    /**
     * Unsigned version of {@code ClickHouseByteValue}.
     */
    static final class UnsignedByteValue extends ClickHouseByteValue {
        protected UnsignedByteValue(boolean isNull, byte value) {
            super(isNull, value);
        }

        @Override
        public short asShort() {
            return (short) (0xFF & getValue());
        }

        @Override
        public int asInteger() {
            return 0xFF & getValue();
        }

        @Override
        public long asLong() {
            return 0xFFL & getValue();
        }

        @Override
        public BigInteger asBigInteger() {
            return isNullOrEmpty() ? null : BigInteger.valueOf(asLong());
        }

        @Override
        public float asFloat() {
            return asInteger();
        }

        @Override
        public double asDouble() {
            return asLong();
        }

        @Override
        public BigDecimal asBigDecimal(int scale) {
            return isNullOrEmpty() ? null : BigDecimal.valueOf(asLong(), scale);
        }

        @Override
        public Object asObject() {
            return isNullOrEmpty() ? null : UnsignedByte.valueOf(getValue());
        }

        @Override
        public String asString() {
            return isNullOrEmpty() ? null : Integer.toString(asInteger());
        }

        @Override
        public ClickHouseByteValue copy(boolean deep) {
            return new UnsignedByteValue(isNullOrEmpty(), getValue());
        }

        @Override
        public String toSqlExpression() {
            return isNullOrEmpty() ? ClickHouseValues.NULL_EXPR : Integer.toString(asInteger());
        }

        @Override
        public ClickHouseByteValue update(BigInteger value) {
            return value == null ? resetToNullOrEmpty() : set(false, value.byteValue());
        }

        @Override
        public ClickHouseByteValue update(BigDecimal value) {
            return value == null ? resetToNullOrEmpty() : set(false, value.byteValue());
        }

        @Override
        public ClickHouseByteValue update(String value) {
            if (value == null) {
                resetToNullOrEmpty();
            } else if (value.isEmpty()) {
                resetToDefault();
            } else {
                set(false, UnsignedByte.valueOf(value).byteValue());
            }
            return this;
        }
    }

    /**
     * Creates a new instance representing null {@code Int8} value.
     *
     * @return new instance representing null value
     */
    public static ClickHouseByteValue ofNull() {
        return ofNull(null, false);
    }

    /**
     * Creates a new instance representing null {@code UInt8} value.
     *
     * @return new instance representing null value
     */
    public static ClickHouseByteValue ofUnsignedNull() {
        return ofNull(null, true);
    }

    /**
     * Creates a new instance representing null value.
     *
     * @param unsigned true if the value is unsigned; false otherwise
     * @return new instance representing null value
     */
    public static ClickHouseByteValue ofNull(boolean unsigned) {
        return ofNull(null, unsigned);
    }

    /**
     * Updates the given value to null or creates a new instance when {@code ref} is
     * null.
     * 
     * @param ref      object to update, could be null
     * @param unsigned true if the value is unsigned; false otherwise
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ClickHouseByteValue ofNull(ClickHouseValue ref, boolean unsigned) {
        if (unsigned) {
            return ref instanceof UnsignedByteValue ? ((UnsignedByteValue) ref).set(true, (byte) 0)
                    : new UnsignedByteValue(true, (byte) 0);
        }
        return ref instanceof ClickHouseByteValue ? ((ClickHouseByteValue) ref).set(true, (byte) 0)
                : new ClickHouseByteValue(true, (byte) 0);
    }

    /**
     * Wraps the given {@code Int8} value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ClickHouseByteValue of(byte value) {
        return of(null, value, false);
    }

    /**
     * Wraps the given {@code UInt8} value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ClickHouseByteValue ofUnsigned(byte value) {
        return of(null, value, true);
    }

    /**
     * Wraps the given value.
     *
     * @param value    value
     * @param unsigned true if {@code value} is unsigned; false otherwise
     * @return object representing the value
     */
    public static ClickHouseByteValue of(byte value, boolean unsigned) {
        return of(null, value, unsigned);
    }

    /**
     * Wraps the given {@code Int8} value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ClickHouseByteValue of(int value) {
        return of(null, (byte) value, false);
    }

    /**
     * Wraps the given {@code UInt8} value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ClickHouseByteValue ofUnsigned(int value) {
        return of(null, (byte) value, true);
    }

    /**
     * Wraps the given value.
     *
     * @param value    value
     * @param unsigned true if {@code value} is unsigned; false otherwise
     * @return object representing the value
     */
    public static ClickHouseByteValue of(int value, boolean unsigned) {
        return of(null, (byte) value, unsigned);
    }

    /**
     * Wraps the given value.
     *
     * @param value    value
     * @param unsigned true if {@code value} is unsigned; false otherwise
     * @return object representing the value
     */
    public static ClickHouseByteValue of(Number value, boolean unsigned) {
        return value == null ? ofNull(null, unsigned) : of(null, value.byteValue(), unsigned);
    }

    /**
     * Updates value of the given object or create a new instance when {@code ref}
     * is null.
     *
     * @param ref      object to update, could be null
     * @param value    value
     * @param unsigned true if {@code value} is unsigned; false otherwise
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ClickHouseByteValue of(ClickHouseValue ref, byte value, boolean unsigned) {
        if (unsigned) {
            return ref instanceof UnsignedByteValue ? ((UnsignedByteValue) ref).set(false, value)
                    : new UnsignedByteValue(false, value);
        }
        return ref instanceof ClickHouseByteValue ? ((ClickHouseByteValue) ref).set(false, value)
                : new ClickHouseByteValue(false, value);
    }

    private boolean isNull;
    private byte value;

    protected ClickHouseByteValue(boolean isNull, byte value) {
        set(isNull, value);
    }

    protected final ClickHouseByteValue set(boolean isNull, byte value) {
        this.isNull = isNull;
        this.value = isNull ? (byte) 0 : value;

        return this;
    }

    /**
     * Gets value.
     *
     * @return value
     */
    public final byte getValue() {
        return value;
    }

    @Override
    public ClickHouseByteValue copy(boolean deep) {
        return new ClickHouseByteValue(isNull, value);
    }

    @Override
    public final boolean isNullOrEmpty() {
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
    public String asString() {
        return isNull ? null : Byte.toString(value);
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
        return isNull ? ClickHouseValues.NULL_EXPR : Byte.toString(value);
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
        if (value == null) {
            resetToNullOrEmpty();
        } else if (value.isEmpty()) {
            resetToDefault();
        } else {
            set(false, Byte.parseByte(value));
        }
        return this;
    }

    @Override
    public ClickHouseByteValue update(ClickHouseValue value) {
        return value == null || value.isNullOrEmpty() ? resetToNullOrEmpty() : set(false, value.asByte());
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

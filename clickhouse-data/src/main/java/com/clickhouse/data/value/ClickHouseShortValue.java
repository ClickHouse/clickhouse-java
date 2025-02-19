package com.clickhouse.data.value;

import java.math.BigDecimal;
import java.math.BigInteger;

import com.clickhouse.data.ClickHouseValue;
import com.clickhouse.data.ClickHouseValues;

/**
 * Wrapper class of {@code short}.
 */
@Deprecated
public class ClickHouseShortValue implements ClickHouseValue {
    /**
     * Unsigned version of {@code ClickHouseShortValue}.
     */
    static final class UnsignedShortValue extends ClickHouseShortValue {
        protected UnsignedShortValue(boolean isNull, short value) {
            super(isNull, value);
        }

        @Override
        public int asInteger() {
            return 0xFFFF & getValue();
        }

        @Override
        public long asLong() {
            return 0xFFFFL & getValue();
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
            return isNullOrEmpty() ? null : UnsignedShort.valueOf(getValue());
        }

        @Override
        public String asString() {
            return isNullOrEmpty() ? null : Integer.toString(asInteger());
        }

        @Override
        public ClickHouseShortValue copy(boolean deep) {
            return new UnsignedShortValue(isNullOrEmpty(), getValue());
        }

        @Override
        public String toSqlExpression() {
            return isNullOrEmpty() ? ClickHouseValues.NULL_EXPR : Integer.toString(asInteger());
        }

        @Override
        public ClickHouseShortValue update(byte value) {
            return set(false, (short) (0xFF & value));
        }

        @Override
        public ClickHouseShortValue update(BigInteger value) {
            return value == null ? resetToNullOrEmpty() : set(false, value.shortValue());
        }

        @Override
        public ClickHouseShortValue update(BigDecimal value) {
            return value == null ? resetToNullOrEmpty() : set(false, value.shortValue());
        }

        @Override
        public ClickHouseShortValue update(String value) {
            if (value == null) {
                resetToNullOrEmpty();
            } else if (value.isEmpty()) {
                resetToDefault();
            } else {
                set(false, UnsignedShort.valueOf(value).shortValue());
            }
            return this;
        }
    }

    /**
     * Creates a new instance representing null {@code Int16} value.
     *
     * @return new instance representing null value
     */
    public static ClickHouseShortValue ofNull() {
        return ofNull(null, false);
    }

    /**
     * Creates a new instance representing null {@code UInt16} value.
     *
     * @return new instance representing null value
     */
    public static ClickHouseShortValue ofUnsignedNull() {
        return ofNull(null, true);
    }

    /**
     * Creates a new instance representing null value.
     *
     * @param unsigned true if the value is unsigned; false otherwise
     * @return new instance representing null value
     */
    public static ClickHouseShortValue ofNull(boolean unsigned) {
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
    public static ClickHouseShortValue ofNull(ClickHouseValue ref, boolean unsigned) {
        if (unsigned) {
            return ref instanceof UnsignedShortValue ? ((UnsignedShortValue) ref).set(true, (short) 0)
                    : new UnsignedShortValue(true, (short) 0);
        }
        return ref instanceof ClickHouseShortValue ? ((ClickHouseShortValue) ref).set(true, (short) 0)
                : new ClickHouseShortValue(true, (short) 0);
    }

    /**
     * Wraps the given {@code Int16} value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ClickHouseShortValue of(short value) {
        return of(null, value, false);
    }

    /**
     * Wraps the given {@code UInt16} value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ClickHouseShortValue ofUnsigned(short value) {
        return of(null, value, true);
    }

    /**
     * Wraps the given value.
     *
     * @param value    value
     * @param unsigned true if {@code value} is unsigned; false otherwise
     * @return object representing the value
     */
    public static ClickHouseShortValue of(short value, boolean unsigned) {
        return of(null, value, unsigned);
    }

    /**
     * Wraps the given {@code Int16} value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ClickHouseShortValue of(int value) {
        return of(null, (short) value, false);
    }

    /**
     * Wraps the given {@code UInt16} value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ClickHouseShortValue ofUnsigned(int value) {
        return of(null, (short) value, true);
    }

    /**
     * Wraps the given value.
     *
     * @param value    value
     * @param unsigned true if {@code value} is unsigned; false otherwise
     * @return object representing the value
     */
    public static ClickHouseShortValue of(int value, boolean unsigned) {
        return of(null, (short) value, unsigned);
    }

    /**
     * Wraps the given value.
     *
     * @param value    value
     * @param unsigned true if {@code value} is unsigned; false otherwise
     * @return object representing the value
     */
    public static ClickHouseShortValue of(Number value, boolean unsigned) {
        return value == null ? ofNull(null, unsigned) : of(null, value.shortValue(), unsigned);
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
    public static ClickHouseShortValue of(ClickHouseValue ref, short value, boolean unsigned) {
        if (unsigned) {
            return ref instanceof UnsignedShortValue ? ((UnsignedShortValue) ref).set(false, value)
                    : new UnsignedShortValue(false, value);
        }
        return ref instanceof ClickHouseShortValue ? ((ClickHouseShortValue) ref).set(false, value)
                : new ClickHouseShortValue(false, value);
    }

    private boolean isNull;
    private short value;

    protected ClickHouseShortValue(boolean isNull, short value) {
        set(isNull, value);
    }

    protected final ClickHouseShortValue set(boolean isNull, short value) {
        this.isNull = isNull;
        this.value = isNull ? (short) 0 : value;

        return this;
    }

    /**
     * Gets value.
     *
     * @return value
     */
    public final short getValue() {
        return value;
    }

    @Override
    public ClickHouseShortValue copy(boolean deep) {
        return new ClickHouseShortValue(isNull, value);
    }

    @Override
    public final boolean isNullOrEmpty() {
        return isNull;
    }

    @Override
    public byte asByte() {
        return (byte) value;
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
        return isNull ? null : getValue();
    }

    @Override
    public String asString() {
        return isNull ? null : Short.toString(value);
    }

    @Override
    public ClickHouseShortValue resetToDefault() {
        return set(false, (short) 0);
    }

    @Override
    public ClickHouseShortValue resetToNullOrEmpty() {
        return set(true, (short) 0);
    }

    @Override
    public String toSqlExpression() {
        return isNullOrEmpty() ? ClickHouseValues.NULL_EXPR : Short.toString(value);
    }

    @Override
    public ClickHouseShortValue update(boolean value) {
        return set(false, value ? (short) 1 : (short) 0);
    }

    @Override
    public ClickHouseShortValue update(char value) {
        return set(false, (short) value);
    }

    @Override
    public ClickHouseShortValue update(byte value) {
        return set(false, value);
    }

    @Override
    public ClickHouseShortValue update(short value) {
        return set(false, value);
    }

    @Override
    public ClickHouseShortValue update(int value) {
        return set(false, (short) value);
    }

    @Override
    public ClickHouseShortValue update(long value) {
        return set(false, (short) value);
    }

    @Override
    public ClickHouseShortValue update(float value) {
        return set(false, (short) value);
    }

    @Override
    public ClickHouseShortValue update(double value) {
        return set(false, (short) value);
    }

    @Override
    public ClickHouseShortValue update(BigInteger value) {
        return value == null ? resetToNullOrEmpty() : set(false, value.shortValueExact());
    }

    @Override
    public ClickHouseShortValue update(BigDecimal value) {
        return value == null ? resetToNullOrEmpty() : set(false, value.shortValueExact());
    }

    @Override
    public ClickHouseShortValue update(Enum<?> value) {
        return value == null ? resetToNullOrEmpty() : set(false, (short) value.ordinal());
    }

    @Override
    public ClickHouseShortValue update(String value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else if (value.isEmpty()) {
            resetToDefault();
        } else {
            set(false, Short.parseShort(value));
        }
        return this;
    }

    @Override
    public ClickHouseShortValue update(ClickHouseValue value) {
        return value == null || value.isNullOrEmpty() ? resetToNullOrEmpty() : set(false, value.asShort());
    }

    @Override
    public ClickHouseShortValue update(Object value) {
        if (value instanceof Number) {
            return set(false, ((Number) value).shortValue());
        } else if (value instanceof ClickHouseValue) {
            return set(false, ((ClickHouseValue) value).asShort());
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

        ClickHouseShortValue v = (ClickHouseShortValue) obj;
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

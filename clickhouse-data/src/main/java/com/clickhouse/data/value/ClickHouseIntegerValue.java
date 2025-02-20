package com.clickhouse.data.value;

import java.math.BigDecimal;
import java.math.BigInteger;
import com.clickhouse.data.ClickHouseValue;
import com.clickhouse.data.ClickHouseValues;

/**
 * Wrapper class of {@code int}.
 */
@Deprecated
public class ClickHouseIntegerValue implements ClickHouseValue {
    /**
     * Unsigned version of {@code ClickHouseIntegerValue}.
     */
    static final class UnsignedIntegerValue extends ClickHouseIntegerValue {
        protected UnsignedIntegerValue(boolean isNull, int value) {
            super(isNull, value);
        }

        @Override
        public long asLong() {
            return 0xFFFFFFFFL & getValue();
        }

        @Override
        public BigInteger asBigInteger() {
            return isNullOrEmpty() ? null : BigInteger.valueOf(asLong());
        }

        @Override
        public float asFloat() {
            return asLong();
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
            return isNullOrEmpty() ? null : UnsignedInteger.valueOf(getValue());
        }

        @Override
        public String asString() {
            return isNullOrEmpty() ? null : Integer.toUnsignedString(getValue());
        }

        @Override
        public ClickHouseIntegerValue copy(boolean deep) {
            return new UnsignedIntegerValue(isNullOrEmpty(), getValue());
        }

        @Override
        public String toSqlExpression() {
            return isNullOrEmpty() ? ClickHouseValues.NULL_EXPR : Integer.toUnsignedString(getValue());
        }

        @Override
        public ClickHouseIntegerValue update(byte value) {
            return set(false, 0xFF & value);
        }

        @Override
        public ClickHouseIntegerValue update(short value) {
            return set(false, 0xFFFF & value);
        }

        @Override
        public ClickHouseIntegerValue update(BigInteger value) {
            return value == null ? resetToNullOrEmpty() : set(false, value.intValue());
        }

        @Override
        public ClickHouseIntegerValue update(BigDecimal value) {
            return value == null ? resetToNullOrEmpty() : set(false, value.intValue());
        }

        @Override
        public ClickHouseIntegerValue update(String value) {
            if (value == null) {
                resetToNullOrEmpty();
            } else if (value.isEmpty()) {
                resetToDefault();
            } else {
                set(false, Integer.parseUnsignedInt(value));
            }
            return this;
        }
    }

    /**
     * Creates a new instance representing null {@code Int32} value.
     *
     * @return new instance representing null value
     */
    public static ClickHouseIntegerValue ofNull() {
        return ofNull(null, false);
    }

    /**
     * Creates a new instance representing null {@code UInt32} value.
     *
     * @return new instance representing null value
     */
    public static ClickHouseIntegerValue ofUnsignedNull() {
        return ofNull(null, true);
    }

    /**
     * Creates a new instance representing null value.
     *
     * @param unsigned true if the value is unsigned; false otherwise
     * @return new instance representing null value
     */
    public static ClickHouseIntegerValue ofNull(boolean unsigned) {
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
    public static ClickHouseIntegerValue ofNull(ClickHouseValue ref, boolean unsigned) {
        if (unsigned) {
            return ref instanceof UnsignedIntegerValue ? ((UnsignedIntegerValue) ref).set(true, 0)
                    : new UnsignedIntegerValue(true, 0);
        }
        return ref instanceof ClickHouseIntegerValue ? ((ClickHouseIntegerValue) ref).set(true, 0)
                : new ClickHouseIntegerValue(true, 0);
    }

    /**
     * Wraps the given {@code Int32} value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ClickHouseIntegerValue of(int value) {
        return of(null, value, false);
    }

    /**
     * Wraps the given {@code UInt32} value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ClickHouseIntegerValue ofUnsigned(int value) {
        return of(null, value, true);
    }

    /**
     * Wraps the given value.
     *
     * @param value    value
     * @param unsigned true if {@code value} is unsigned; false otherwise
     * @return object representing the value
     */
    public static ClickHouseIntegerValue of(int value, boolean unsigned) {
        return of(null, value, unsigned);
    }

    /**
     * Wraps the given value.
     *
     * @param value    value
     * @param unsigned true if {@code value} is unsigned; false otherwise
     * @return object representing the value
     */
    public static ClickHouseIntegerValue of(Number value, boolean unsigned) {
        return value == null ? ofNull(null, unsigned) : of(null, value.intValue(), unsigned);
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
    public static ClickHouseIntegerValue of(ClickHouseValue ref, int value, boolean unsigned) {
        if (unsigned) {
            return ref instanceof UnsignedIntegerValue ? ((UnsignedIntegerValue) ref).set(false, value)
                    : new UnsignedIntegerValue(false, value);
        }
        return ref instanceof ClickHouseIntegerValue ? ((ClickHouseIntegerValue) ref).set(false, value)
                : new ClickHouseIntegerValue(false, value);
    }

    private boolean isNull;
    private int value;

    protected ClickHouseIntegerValue(boolean isNull, int value) {
        set(isNull, value);
    }

    protected final ClickHouseIntegerValue set(boolean isNull, int value) {
        this.isNull = isNull;
        this.value = isNull ? 0 : value;

        return this;
    }

    /**
     * Gets value.
     *
     * @return value
     */
    public final int getValue() {
        return value;
    }

    @Override
    public ClickHouseIntegerValue copy(boolean deep) {
        return new ClickHouseIntegerValue(isNull, value);
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
    public String asString() {
        return isNull ? null : Integer.toString(value);
    }

    @Override
    public ClickHouseIntegerValue resetToDefault() {
        return set(false, 0);
    }

    @Override
    public ClickHouseIntegerValue resetToNullOrEmpty() {
        return set(true, 0);
    }

    @Override
    public String toSqlExpression() {
        return isNullOrEmpty() ? ClickHouseValues.NULL_EXPR : Integer.toString(value);
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
        if (value == null) {
            resetToNullOrEmpty();
        } else if (value.isEmpty()) {
            resetToDefault();
        } else {
            set(false, Integer.parseInt(value));
        }
        return this;
    }

    @Override
    public ClickHouseIntegerValue update(ClickHouseValue value) {
        return value == null || value.isNullOrEmpty() ? resetToNullOrEmpty() : set(false, value.asInteger());
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

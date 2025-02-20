package com.clickhouse.data.value;

import java.math.BigDecimal;
import java.math.BigInteger;
import com.clickhouse.data.ClickHouseValue;
import com.clickhouse.data.ClickHouseValues;

/**
 * Wrapper class of {@code long}.
 */
@Deprecated
public class ClickHouseLongValue implements ClickHouseValue {
    /**
     * Unsigned version of {@code ClickHouseLongValue}.
     */
    static final class UnsignedLongValue extends ClickHouseLongValue {
        protected UnsignedLongValue(boolean isNull, long value) {
            super(isNull, value);
        }

        @Override
        public BigInteger asBigInteger() {
            if (isNullOrEmpty()) {
                return null;
            }

            long l = getValue();
            BigInteger v = BigInteger.valueOf(l);
            if (l < 0L) {
                v = v.and(UnsignedLong.MASK);
            }
            return v;
        }

        @Override
        public float asFloat() {
            return getValue();
        }

        @Override
        public double asDouble() {
            return getValue();
        }

        @Override
        public BigDecimal asBigDecimal(int scale) {
            if (isNullOrEmpty()) {
                return null;
            }

            long l = getValue();
            return l < 0L ? new BigDecimal(BigInteger.valueOf(l).and(UnsignedLong.MASK), scale)
                    : BigDecimal.valueOf(l, scale);
        }

        @Override
        public Object asObject() {
            return isNullOrEmpty() ? null : UnsignedLong.valueOf(getValue());
        }

        @Override
        public String asString() {
            return isNullOrEmpty() ? null : Long.toUnsignedString(getValue());
        }

        @Override
        public ClickHouseLongValue copy(boolean deep) {
            return new UnsignedLongValue(isNullOrEmpty(), getValue());
        }

        @Override
        public String toSqlExpression() {
            return isNullOrEmpty() ? ClickHouseValues.NULL_EXPR : Long.toUnsignedString(getValue());
        }

        @Override
        public ClickHouseLongValue update(byte value) {
            return set(false, 0xFFL & value);
        }

        @Override
        public ClickHouseLongValue update(short value) {
            return set(false, 0xFFFFL & value);
        }

        @Override
        public ClickHouseLongValue update(int value) {
            return set(false, 0xFFFFFFFFL & value);
        }

        @Override
        public ClickHouseLongValue update(BigInteger value) {
            return value == null ? resetToNullOrEmpty() : set(false, value.longValue());
        }

        @Override
        public ClickHouseLongValue update(BigDecimal value) {
            return value == null ? resetToNullOrEmpty() : set(false, value.longValue());
        }

        @Override
        public ClickHouseLongValue update(String value) {
            if (value == null) {
                resetToNullOrEmpty();
            } else if (value.isEmpty()) {
                resetToDefault();
            } else {
                set(false, Long.parseUnsignedLong(value));
            }
            return this;
        }
    }

    /**
     * Creates a new instance representing null {@code Int64} value.
     *
     * @return new instance representing null value
     */
    public static ClickHouseLongValue ofNull() {
        return ofNull(null, false);
    }

    /**
     * Creates a new instance representing null {@code UInt64} value.
     *
     * @return new instance representing null value
     */
    public static ClickHouseLongValue ofUnsignedNull() {
        return ofNull(null, true);
    }

    /**
     * Creates a new instance representing null value.
     *
     * @param unsigned true if the value is unsigned; false otherwise
     * @return new instance representing null value
     */
    public static ClickHouseLongValue ofNull(boolean unsigned) {
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
    public static ClickHouseLongValue ofNull(ClickHouseValue ref, boolean unsigned) {
        if (unsigned) {
            return ref instanceof UnsignedLongValue ? ((UnsignedLongValue) ref).set(true, 0L)
                    : new UnsignedLongValue(true, 0L);
        }
        return ref instanceof ClickHouseLongValue ? ((ClickHouseLongValue) ref).set(true, 0L)
                : new ClickHouseLongValue(true, 0L);
    }

    /**
     * Wraps the given {@code Int64} value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ClickHouseLongValue of(long value) {
        return of(null, value, false);
    }

    /**
     * Wraps the given {@code UInt64} value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ClickHouseLongValue ofUnsigned(long value) {
        return of(null, value, true);
    }

    /**
     * Wraps the given value.
     *
     * @param value    value
     * @param unsigned true if {@code value} is unsigned; false otherwise
     * @return object representing the value
     */
    public static ClickHouseLongValue of(long value, boolean unsigned) {
        return of(null, value, unsigned);
    }

    /**
     * Wraps the given {@code Int64} value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ClickHouseLongValue of(int value) {
        return of(null, value, false);
    }

    /**
     * Wraps the given {@code UInt64} value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ClickHouseLongValue ofUnsigned(int value) {
        return of(null, 0xFFFFFFFFL & value, true);
    }

    /**
     * Wraps the given value.
     *
     * @param value    value
     * @param unsigned true if {@code value} is unsigned; false otherwise
     * @return object representing the value
     */
    public static ClickHouseLongValue of(int value, boolean unsigned) {
        return of(null, unsigned ? 0xFFFFFFFFL & value : value, unsigned);
    }

    /**
     * Wraps the given value.
     *
     * @param value    value
     * @param unsigned true if {@code value} is unsigned; false otherwise
     * @return object representing the value
     */
    public static ClickHouseLongValue of(Number value, boolean unsigned) {
        return value == null ? ofNull(null, unsigned) : of(null, value.longValue(), unsigned);
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
    public static ClickHouseLongValue of(ClickHouseValue ref, long value, boolean unsigned) {
        if (unsigned) {
            return ref instanceof UnsignedLongValue ? ((UnsignedLongValue) ref).set(false, value)
                    : new UnsignedLongValue(false, value);
        }
        return ref instanceof ClickHouseLongValue ? ((ClickHouseLongValue) ref).set(false, value)
                : new ClickHouseLongValue(false, value);
    }

    private boolean isNull;
    private long value;

    protected ClickHouseLongValue(boolean isNull, long value) {
        set(isNull, value);
    }

    protected final ClickHouseLongValue set(boolean isNull, long value) {
        this.isNull = isNull;
        this.value = isNull ? 0L : value;

        return this;
    }

    /**
     * Gets value.
     *
     * @return value
     */
    public final long getValue() {
        return value;
    }

    @Override
    public ClickHouseLongValue copy(boolean deep) {
        return new ClickHouseLongValue(isNull, value);
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
        return (int) value;
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
        return isNull ? null : Long.toString(value);
    }

    @Override
    public ClickHouseLongValue resetToDefault() {
        return set(false, 0L);
    }

    @Override
    public ClickHouseLongValue resetToNullOrEmpty() {
        return set(true, 0L);
    }

    @Override
    public String toSqlExpression() {
        return isNull ? ClickHouseValues.NULL_EXPR : Long.toString(value);
    }

    @Override
    public ClickHouseLongValue update(boolean value) {
        return set(false, value ? 1L : 0L);
    }

    @Override
    public ClickHouseLongValue update(char value) {
        return set(false, value);
    }

    @Override
    public ClickHouseLongValue update(byte value) {
        return set(false, value);
    }

    @Override
    public ClickHouseLongValue update(short value) {
        return set(false, value);
    }

    @Override
    public ClickHouseLongValue update(int value) {
        return set(false, value);
    }

    @Override
    public ClickHouseLongValue update(long value) {
        return set(false, value);
    }

    @Override
    public ClickHouseLongValue update(float value) {
        return set(false, (long) value);
    }

    @Override
    public ClickHouseLongValue update(double value) {
        return set(false, (long) value);
    }

    @Override
    public ClickHouseLongValue update(BigInteger value) {
        return value == null ? resetToNullOrEmpty() : set(false, value.longValueExact());
    }

    @Override
    public ClickHouseLongValue update(BigDecimal value) {
        return value == null ? resetToNullOrEmpty() : set(false, value.longValueExact());
    }

    @Override
    public ClickHouseLongValue update(Enum<?> value) {
        return value == null ? resetToNullOrEmpty() : set(false, value.ordinal());
    }

    @Override
    public ClickHouseLongValue update(String value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else if (value.isEmpty()) {
            resetToDefault();
        } else {
            set(false, Long.parseLong(value));
        }
        return this;
    }

    @Override
    public ClickHouseLongValue update(ClickHouseValue value) {
        return value == null || value.isNullOrEmpty() ? resetToNullOrEmpty() : set(false, value.asLong());
    }

    @Override
    public ClickHouseLongValue update(Object value) {
        if (value instanceof Number) {
            return set(false, ((Number) value).longValue());
        } else if (value instanceof ClickHouseValue) {
            return set(false, ((ClickHouseValue) value).asLong());
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

        ClickHouseLongValue v = (ClickHouseLongValue) obj;
        return isNull == v.isNull && value == v.value;
    }

    @Override
    public int hashCode() {
        // not going to use Objects.hash(isNull, value) due to autoboxing
        return (31 + (isNull ? 1231 : 1237)) * 31 + (int) (value ^ (value >>> 32));
    }

    @Override
    public String toString() {
        return ClickHouseValues.convertToString(this);
    }
}

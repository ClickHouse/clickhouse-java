package com.clickhouse.data.value;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.UUID;
import com.clickhouse.data.ClickHouseValue;
import com.clickhouse.data.ClickHouseValues;

/**
 * Wrapper class of {@link BigInteger}.
 */
@Deprecated
public class ClickHouseBigIntegerValue extends ClickHouseObjectValue<BigInteger> {
    /**
     * Create a new instance representing null value.
     *
     * @return new instance representing null value
     */
    public static ClickHouseBigIntegerValue ofNull() {
        return ofNull(null);
    }

    /**
     * Update given value to null or create a new instance if {@code ref} is null.
     * 
     * @param ref object to update, could be null
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ClickHouseBigIntegerValue ofNull(ClickHouseValue ref) {
        return ref instanceof ClickHouseBigIntegerValue
                ? (ClickHouseBigIntegerValue) ((ClickHouseBigIntegerValue) ref).set(null)
                : new ClickHouseBigIntegerValue(null);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ClickHouseBigIntegerValue of(BigInteger value) {
        return of(null, value);
    }

    /**
     * Update value of the given object or create a new instance if {@code ref} is
     * null.
     *
     * @param ref   object to update, could be null
     * @param value value
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ClickHouseBigIntegerValue of(ClickHouseValue ref, BigInteger value) {
        return ref instanceof ClickHouseBigIntegerValue
                ? (ClickHouseBigIntegerValue) ((ClickHouseBigIntegerValue) ref).set(value)
                : new ClickHouseBigIntegerValue(value);
    }

    protected ClickHouseBigIntegerValue(BigInteger value) {
        super(value);
    }

    @Override
    public ClickHouseBigIntegerValue copy(boolean deep) {
        return new ClickHouseBigIntegerValue(getValue());
    }

    @Override
    public byte asByte() {
        return isNullOrEmpty() ? (byte) 0 : getValue().byteValue();
    }

    @Override
    public short asShort() {
        return isNullOrEmpty() ? (short) 0 : getValue().shortValue();
    }

    @Override
    public int asInteger() {
        return isNullOrEmpty() ? 0 : getValue().intValue();
    }

    @Override
    public long asLong() {
        return isNullOrEmpty() ? 0L : getValue().longValue();
    }

    @Override
    public BigInteger asBigInteger() {
        return getValue();
    }

    @Override
    public float asFloat() {
        return isNullOrEmpty() ? 0F : getValue().floatValue();
    }

    @Override
    public double asDouble() {
        return isNullOrEmpty() ? 0D : getValue().doubleValue();
    }

    @Override
    public BigDecimal asBigDecimal(int scale) {
        return isNullOrEmpty() ? null : new BigDecimal(getValue(), scale);
    }

    @Override
    public Object asObject() {
        return getValue();
    }

    @Override
    public ClickHouseBigIntegerValue resetToDefault() {
        set(BigInteger.ZERO);
        return this;
    }

    @Override
    public String toSqlExpression() {
        return isNullOrEmpty() ? ClickHouseValues.NULL_EXPR : String.valueOf(getValue());
    }

    @Override
    public ClickHouseBigIntegerValue update(boolean value) {
        set(value ? BigInteger.ONE : BigInteger.ZERO);
        return this;
    }

    @Override
    public ClickHouseBigIntegerValue update(char value) {
        set(BigInteger.valueOf(value));
        return this;
    }

    @Override
    public ClickHouseBigIntegerValue update(byte value) {
        set(BigInteger.valueOf(value));
        return this;
    }

    @Override
    public ClickHouseBigIntegerValue update(short value) {
        set(BigInteger.valueOf(value));
        return this;
    }

    @Override
    public ClickHouseBigIntegerValue update(int value) {
        set(BigInteger.valueOf(value));
        return this;
    }

    @Override
    public ClickHouseBigIntegerValue update(long value) {
        set(BigInteger.valueOf(value));
        return this;
    }

    @Override
    public ClickHouseBigIntegerValue update(float value) {
        set(BigDecimal.valueOf(value).toBigInteger());
        return this;
    }

    @Override
    public ClickHouseBigIntegerValue update(double value) {
        set(BigDecimal.valueOf(value).toBigInteger());
        return this;
    }

    @Override
    public ClickHouseBigIntegerValue update(BigInteger value) {
        set(value);
        return this;
    }

    @Override
    public ClickHouseBigIntegerValue update(BigDecimal value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(value.toBigIntegerExact());
        }
        return this;
    }

    @Override
    public ClickHouseBigIntegerValue update(Enum<?> value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(BigInteger.valueOf(value.ordinal()));
        }
        return this;
    }

    @Override
    public ClickHouseBigIntegerValue update(Inet4Address value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(ClickHouseValues.convertToBigInteger(value));
        }
        return this;
    }

    @Override
    public ClickHouseBigIntegerValue update(Inet6Address value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(ClickHouseValues.convertToBigInteger(value));
        }
        return this;
    }

    @Override
    public ClickHouseBigIntegerValue update(LocalDate value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(BigInteger.valueOf(value.toEpochDay()));
        }
        return this;
    }

    @Override
    public ClickHouseBigIntegerValue update(LocalTime value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(BigInteger.valueOf(value.toSecondOfDay()));
        }
        return this;
    }

    @Override
    public ClickHouseBigIntegerValue update(LocalDateTime value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(BigInteger.valueOf(value.toEpochSecond(ZoneOffset.UTC)));
        }
        return this;
    }

    @Override
    public ClickHouseBigIntegerValue update(String value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else if (value.isEmpty()) {
            resetToDefault();
        } else {
            set(new BigInteger(value));
        }
        return this;
    }

    @Override
    public ClickHouseBigIntegerValue update(UUID value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(ClickHouseValues.convertToBigInteger(value));
        }
        return this;
    }

    @Override
    public ClickHouseBigIntegerValue update(ClickHouseValue value) {
        if (value == null || value.isNullOrEmpty()) {
            resetToNullOrEmpty();
        } else {
            set(value.asBigInteger());
        }
        return this;
    }

    @Override
    public ClickHouseBigIntegerValue update(Object value) {
        if (value instanceof BigInteger) {
            set((BigInteger) value);
            return this;
        }

        super.update(value);
        return this;
    }
}

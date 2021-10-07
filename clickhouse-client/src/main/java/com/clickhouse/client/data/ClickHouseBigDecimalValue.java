package com.clickhouse.client.data;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.UUID;
import com.clickhouse.client.ClickHouseValue;
import com.clickhouse.client.ClickHouseValues;

/**
 * Wraper class of BigDecimal.
 */
public class ClickHouseBigDecimalValue extends ClickHouseObjectValue<BigDecimal> {
    /**
     * Create a new instance representing null value.
     *
     * @return new instance representing null value
     */
    public static ClickHouseBigDecimalValue ofNull() {
        return ofNull(null);
    }

    /**
     * Update given value to null or create a new instance if {@code ref} is null.
     * 
     * @param ref object to update, could be null
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ClickHouseBigDecimalValue ofNull(ClickHouseValue ref) {
        return ref instanceof ClickHouseBigDecimalValue
                ? (ClickHouseBigDecimalValue) ((ClickHouseBigDecimalValue) ref).set(null)
                : new ClickHouseBigDecimalValue(null);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ClickHouseBigDecimalValue of(BigDecimal value) {
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
    public static ClickHouseBigDecimalValue of(ClickHouseValue ref, BigDecimal value) {
        return ref instanceof ClickHouseBigDecimalValue
                ? (ClickHouseBigDecimalValue) ((ClickHouseBigDecimalValue) ref).set(value)
                : new ClickHouseBigDecimalValue(value);
    }

    protected ClickHouseBigDecimalValue(BigDecimal value) {
        super(value);
    }

    @Override
    public ClickHouseBigDecimalValue copy(boolean deep) {
        return new ClickHouseBigDecimalValue(getValue());
    }

    @Override
    public byte asByte() {
        return isNullOrEmpty() ? (byte) 0 : getValue().byteValueExact();
    }

    @Override
    public short asShort() {
        return isNullOrEmpty() ? (short) 0 : getValue().shortValueExact();
    }

    @Override
    public int asInteger() {
        return isNullOrEmpty() ? 0 : getValue().intValueExact();
    }

    @Override
    public long asLong() {
        return isNullOrEmpty() ? 0L : getValue().longValueExact();
    }

    @Override
    public BigInteger asBigInteger() {
        if (isNullOrEmpty()) {
            return null;
        }

        BigDecimal value = getValue();
        if (value.remainder(BigDecimal.ONE) != BigDecimal.ZERO) {
            throw new IllegalArgumentException("Failed to convert BigDecimal to BigInteger: " + value);
        }

        return value.toBigInteger();
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
        return getValue().setScale(scale);
    }

    @Override
    public Object asObject() {
        return getValue();
    }

    public int getScale() {
        return isNullOrEmpty() ? 0 : getValue().scale();
    }

    @Override
    public String toSqlExpression() {
        return isNullOrEmpty() ? ClickHouseValues.NULL_EXPR : String.valueOf(getValue());
    }

    @Override
    public ClickHouseBigDecimalValue update(boolean value) {
        set(value ? BigDecimal.ONE : BigDecimal.ZERO);
        return this;
    }

    @Override
    public ClickHouseBigDecimalValue update(char value) {
        set(BigDecimal.valueOf(value));
        return this;
    }

    @Override
    public ClickHouseBigDecimalValue update(byte value) {
        set(BigDecimal.valueOf(value));
        return this;
    }

    @Override
    public ClickHouseBigDecimalValue update(short value) {
        set(BigDecimal.valueOf(value));
        return this;
    }

    @Override
    public ClickHouseBigDecimalValue update(int value) {
        set(BigDecimal.valueOf(value));
        return this;
    }

    @Override
    public ClickHouseBigDecimalValue update(long value) {
        set(BigDecimal.valueOf(value));
        return this;
    }

    @Override
    public ClickHouseBigDecimalValue update(float value) {
        set(BigDecimal.valueOf(value));
        return this;
    }

    @Override
    public ClickHouseBigDecimalValue update(double value) {
        set(BigDecimal.valueOf(value));
        return this;
    }

    @Override
    public ClickHouseBigDecimalValue update(BigInteger value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(new BigDecimal(value));
        }
        return this;
    }

    @Override
    public ClickHouseBigDecimalValue update(BigDecimal value) {
        set(value);
        return this;
    }

    @Override
    public ClickHouseBigDecimalValue update(Enum<?> value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(BigDecimal.valueOf(value.ordinal()));
        }
        return this;
    }

    @Override
    public ClickHouseBigDecimalValue update(Inet4Address value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(new BigDecimal(ClickHouseValues.convertToBigInteger(value)));
        }
        return this;
    }

    @Override
    public ClickHouseBigDecimalValue update(Inet6Address value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(new BigDecimal(ClickHouseValues.convertToBigInteger(value)));
        }
        return this;
    }

    @Override
    public ClickHouseBigDecimalValue update(LocalDate value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(BigDecimal.valueOf(value.toEpochDay()));
        }
        return this;
    }

    @Override
    public ClickHouseBigDecimalValue update(LocalTime value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(BigDecimal.valueOf(value.toSecondOfDay()));
        }
        return this;
    }

    @Override
    public ClickHouseBigDecimalValue update(LocalDateTime value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(BigDecimal.valueOf(value.toEpochSecond(ZoneOffset.UTC)));
        }
        return this;
    }

    @Override
    public ClickHouseBigDecimalValue update(String value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(new BigDecimal(value));
        }
        return this;
    }

    @Override
    public ClickHouseBigDecimalValue update(UUID value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(new BigDecimal(ClickHouseValues.convertToBigInteger(value)));
        }
        return this;
    }

    @Override
    public ClickHouseBigDecimalValue update(ClickHouseValue value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(value.asBigDecimal(getScale()));
        }
        return this;
    }

    @Override
    public ClickHouseBigDecimalValue update(Object value) {
        if (value instanceof BigDecimal) {
            set((BigDecimal) value);
            return this;
        }

        super.update(value);
        return this;
    }
}

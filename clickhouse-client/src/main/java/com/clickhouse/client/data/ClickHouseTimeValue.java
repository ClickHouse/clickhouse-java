package com.clickhouse.client.data;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import com.clickhouse.client.ClickHouseChecker;
import com.clickhouse.client.ClickHouseValue;
import com.clickhouse.client.ClickHouseValues;

/**
 * Wraper class of LocalTime.
 */
public class ClickHouseTimeValue extends ClickHouseObjectValue<LocalTime> {
    /**
     * Create a new instance representing null value.
     *
     * @return new instance representing null value
     */
    public static ClickHouseTimeValue ofNull() {
        return ofNull(null);
    }

    /**
     * Update given value to null or create a new instance if {@code ref} is null.
     * 
     * @param ref object to update, could be null
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ClickHouseTimeValue ofNull(ClickHouseValue ref) {
        return ref instanceof ClickHouseTimeValue ? (ClickHouseTimeValue) ((ClickHouseTimeValue) ref).set(null)
                : new ClickHouseTimeValue(null);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ClickHouseTimeValue of(LocalTime value) {
        return of(null, value);
    }

    /**
     * Wrap the given value.
     *
     * @param secondOfDay second of day
     * @return object representing the value
     */
    public static ClickHouseTimeValue of(long secondOfDay) {
        // what about nano second?
        return of(null, LocalTime.ofSecondOfDay(secondOfDay));
    }

    /**
     * Update value of the given object or create a new instance if {@code ref} is
     * null.
     *
     * @param ref   object to update, could be null
     * @param value value
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ClickHouseTimeValue of(ClickHouseValue ref, LocalTime value) {
        return ref instanceof ClickHouseTimeValue ? (ClickHouseTimeValue) ((ClickHouseTimeValue) ref).update(value)
                : new ClickHouseTimeValue(value);
    }

    protected ClickHouseTimeValue(LocalTime value) {
        super(value);
    }

    @Override
    public ClickHouseTimeValue copy(boolean deep) {
        return new ClickHouseTimeValue(getValue());
    }

    @Override
    public byte asByte() {
        return (byte) asInteger();
    }

    @Override
    public short asShort() {
        return (short) asInteger();
    }

    @Override
    public int asInteger() {
        return isNullOrEmpty() ? 0 : getValue().toSecondOfDay();
    }

    @Override
    public long asLong() {
        return asInteger();
    }

    @Override
    public BigInteger asBigInteger() {
        return isNullOrEmpty() ? null : BigInteger.valueOf(getValue().toSecondOfDay());
    }

    @Override
    public float asFloat() {
        return asInteger();
    }

    @Override
    public double asDouble() {
        return asInteger();
    }

    @Override
    public BigDecimal asBigDecimal(int scale) {
        return isNullOrEmpty() ? null : new BigDecimal(BigInteger.valueOf(getValue().toSecondOfDay()), scale);
    }

    @Override
    public LocalDate asDate() {
        return isNullOrEmpty() ? null : ClickHouseValues.DATE_ZERO;
    }

    @Override
    public LocalTime asTime() {
        return getValue();
    }

    @Override
    public LocalDateTime asDateTime(int scale) {
        return isNullOrEmpty() ? null : LocalDateTime.of(ClickHouseValues.DATE_ZERO, getValue());
    }

    @Override
    public String asString(int length, Charset charset) {
        if (isNullOrEmpty()) {
            return null;
        }

        String str = getValue().format(ClickHouseValues.TIME_FORMATTER);
        if (length > 0) {
            ClickHouseChecker.notWithDifferentLength(str.getBytes(charset == null ? StandardCharsets.UTF_8 : charset),
                    length);
        }

        return str;
    }

    @Override
    public String toSqlExpression() {
        if (isNullOrEmpty()) {
            return ClickHouseValues.NULL_EXPR;
        }
        return new StringBuilder().append('\'').append(getValue().format(ClickHouseValues.TIME_FORMATTER)).append('\'')
                .toString();
    }

    @Override
    public ClickHouseTimeValue update(byte value) {
        set(LocalTime.ofSecondOfDay(value));
        return this;
    }

    @Override
    public ClickHouseTimeValue update(short value) {
        set(LocalTime.ofSecondOfDay(value));
        return this;
    }

    @Override
    public ClickHouseTimeValue update(int value) {
        set(LocalTime.ofSecondOfDay(value));
        return this;
    }

    @Override
    public ClickHouseTimeValue update(long value) {
        set(LocalTime.ofSecondOfDay(value));
        return this;
    }

    @Override
    public ClickHouseTimeValue update(float value) {
        set(LocalTime.ofSecondOfDay((long) value));
        return this;
    }

    @Override
    public ClickHouseTimeValue update(double value) {
        set(LocalTime.ofSecondOfDay((long) value));
        return this;
    }

    @Override
    public ClickHouseTimeValue update(BigInteger value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(LocalTime.ofSecondOfDay(value.longValueExact()));
        }
        return this;
    }

    @Override
    public ClickHouseTimeValue update(BigDecimal value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(LocalTime.ofSecondOfDay(value.longValueExact()));
        }
        return this;
    }

    @Override
    public ClickHouseTimeValue update(Enum<?> value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(LocalTime.ofSecondOfDay(value.ordinal()));
        }
        return this;
    }

    @Override
    public ClickHouseTimeValue update(LocalDate value) {
        return this;
    }

    @Override
    public ClickHouseTimeValue update(LocalTime value) {
        set(value);
        return this;
    }

    @Override
    public ClickHouseTimeValue update(LocalDateTime value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(value.toLocalTime());
        }
        return this;
    }

    @Override
    public ClickHouseTimeValue update(String value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(LocalTime.parse(value, ClickHouseValues.TIME_FORMATTER));
        }
        return this;
    }

    @Override
    public ClickHouseTimeValue update(ClickHouseValue value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(value.asTime());
        }
        return this;
    }

    @Override
    public ClickHouseTimeValue update(Object value) {
        if (value instanceof LocalTime) {
            set((LocalTime) value);
        } else if (value instanceof LocalDateTime) {
            set(((LocalDateTime) value).toLocalTime());
        } else if (value instanceof LocalDate) {
            set(LocalTime.MIN);
        } else {
            super.update(value);
        }
        return this;
    }
}

package com.clickhouse.data.value;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import com.clickhouse.data.ClickHouseValue;
import com.clickhouse.data.ClickHouseValues;

/**
 * Wrapper class of {@link LocalDate}.
 */
@Deprecated
public class ClickHouseDateValue extends ClickHouseObjectValue<LocalDate> {
    /**
     * Default date.
     */
    public static final LocalDate DEFAULT = ClickHouseOffsetDateTimeValue.DEFAULT.toLocalDate();

    /**
     * Create a new instance representing null value.
     *
     * @return new instance representing null value
     */
    public static ClickHouseDateValue ofNull() {
        return ofNull(null);
    }

    /**
     * Update given value to null or create a new instance if {@code ref} is null.
     * 
     * @param ref object to update, could be null
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ClickHouseDateValue ofNull(ClickHouseValue ref) {
        return ref instanceof ClickHouseDateValue ? (ClickHouseDateValue) ((ClickHouseDateValue) ref).set(null)
                : new ClickHouseDateValue(null);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ClickHouseDateValue of(LocalDate value) {
        return of(null, value);
    }

    /**
     * Wrap the given value.
     *
     * @param epochDay epoch day
     * @return object representing the value
     */
    public static ClickHouseDateValue of(long epochDay) {
        return of(null, LocalDate.ofEpochDay(epochDay));
    }

    /**
     * Update value of the given object or create a new instance if {@code ref} is
     * null.
     *
     * @param ref   object to update, could be null
     * @param value value
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ClickHouseDateValue of(ClickHouseValue ref, LocalDate value) {
        return ref instanceof ClickHouseDateValue ? (ClickHouseDateValue) ((ClickHouseDateValue) ref).update(value)
                : new ClickHouseDateValue(value);
    }

    protected ClickHouseDateValue(LocalDate value) {
        super(value);
    }

    @Override
    public ClickHouseDateValue copy(boolean deep) {
        return new ClickHouseDateValue(getValue());
    }

    @Override
    public byte asByte() {
        return (byte) asLong();
    }

    @Override
    public short asShort() {
        return (short) asLong();
    }

    @Override
    public int asInteger() {
        return (int) asLong();
    }

    @Override
    public long asLong() {
        return isNullOrEmpty() ? 0L : getValue().toEpochDay();
    }

    @Override
    public BigInteger asBigInteger() {
        return isNullOrEmpty() ? null : BigInteger.valueOf(getValue().toEpochDay());
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
        return isNullOrEmpty() ? null : new BigDecimal(asBigInteger(), scale);
    }

    @Override
    public LocalDate asDate() {
        return getValue();
    }

    @Override
    public final LocalTime asTime(int scale) {
        return isNullOrEmpty() ? null : LocalTime.ofSecondOfDay(0L);
    }

    @Override
    public LocalDateTime asDateTime(int scale) {
        if (isNullOrEmpty()) {
            return null;
        }

        return LocalDateTime.of(getValue(), ClickHouseValues.TIME_ZERO);
    }

    @Override
    public String asString() {
        if (isNullOrEmpty()) {
            return null;
        }

        return asDate().format(ClickHouseValues.DATE_FORMATTER);
    }

    @Override
    public ClickHouseDateValue resetToDefault() {
        set(DEFAULT);
        return this;
    }

    @Override
    public String toSqlExpression() {
        if (isNullOrEmpty()) {
            return ClickHouseValues.NULL_EXPR;
        }
        return new StringBuilder().append('\'').append(asDate().format(ClickHouseValues.DATE_FORMATTER)).append('\'')
                .toString();
    }

    @Override
    public ClickHouseDateValue update(byte value) {
        set(LocalDate.ofEpochDay(value));
        return this;
    }

    @Override
    public ClickHouseDateValue update(short value) {
        set(LocalDate.ofEpochDay(value));
        return this;
    }

    @Override
    public ClickHouseDateValue update(int value) {
        set(LocalDate.ofEpochDay(value));
        return this;
    }

    @Override
    public ClickHouseDateValue update(long value) {
        set(LocalDate.ofEpochDay(value));
        return this;
    }

    @Override
    public ClickHouseDateValue update(float value) {
        set(LocalDate.ofEpochDay((long) value));
        return this;
    }

    @Override
    public ClickHouseDateValue update(double value) {
        set(LocalDate.ofEpochDay((long) value));
        return this;
    }

    @Override
    public ClickHouseDateValue update(BigInteger value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(LocalDate.ofEpochDay(value.longValue()));
        }
        return this;
    }

    @Override
    public ClickHouseDateValue update(BigDecimal value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(LocalDate.ofEpochDay(value.longValue()));
        }
        return this;
    }

    @Override
    public ClickHouseDateValue update(Enum<?> value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(LocalDate.ofEpochDay(value.ordinal()));
        }
        return this;
    }

    @Override
    public ClickHouseDateValue update(LocalDate value) {
        set(value);
        return this;
    }

    @Override
    public ClickHouseDateValue update(LocalTime value) {
        return this;
    }

    @Override
    public ClickHouseDateValue update(LocalDateTime value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(value.toLocalDate());
        }
        return this;
    }

    @Override
    public ClickHouseDateValue update(String value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else if (value.isEmpty()) {
            resetToDefault();
        } else {
            set(LocalDate.parse(value, ClickHouseValues.DATE_FORMATTER));
        }
        return this;
    }

    @Override
    public ClickHouseDateValue update(ClickHouseValue value) {
        if (value == null || value.isNullOrEmpty()) {
            resetToNullOrEmpty();
        } else {
            set(value.asDate());
        }
        return this;
    }

    @Override
    public ClickHouseDateValue update(Object value) {
        if (value instanceof LocalDate) {
            set((LocalDate) value);
        } else if (value instanceof LocalDateTime) {
            set(((LocalDateTime) value).toLocalDate());
        } else if (value instanceof LocalTime) {
            set(LocalDate.now());
        } else {
            super.update(value);
        }
        return this;
    }
}

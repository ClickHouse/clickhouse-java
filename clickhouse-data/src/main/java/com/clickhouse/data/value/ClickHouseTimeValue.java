package com.clickhouse.data.value;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;
import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseDataConfig;
import com.clickhouse.data.ClickHouseValue;
import com.clickhouse.data.ClickHouseValues;

/**
 * Wrapper class of {@link LocalTime} for Time type (32-bit seconds since midnight).
 */
public class ClickHouseTimeValue extends ClickHouseObjectValue<LocalTime> {
    /**
     * Default value.
     */
    public static final LocalTime DEFAULT = LocalTime.ofSecondOfDay(0L);

    static final DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss");

    /**
     * Create a new instance representing null getValue().
     *
     * @param tz time zone, null is treated as {@code UTC}
     * @return new instance representing null value
     */
    public static ClickHouseTimeValue ofNull(TimeZone tz) {
        return ofNull(null, tz);
    }

    /**
     * Update given value to null or create a new instance if {@code ref} is null.
     * 
     * @param ref object to update, could be null
     * @param tz  time zone, null is treated as {@code UTC}
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ClickHouseTimeValue ofNull(ClickHouseValue ref, TimeZone tz) {
        return ref instanceof ClickHouseTimeValue
                ? (ClickHouseTimeValue) ((ClickHouseTimeValue) ref).set(null)
                : new ClickHouseTimeValue(null, tz);
    }

    /**
     * Wrap the given getValue().
     *
     * @param value value
     * @param tz    time zone, null is treated as {@code UTC}
     * @return object representing the value
     */
    public static ClickHouseTimeValue of(LocalTime value, TimeZone tz) {
        return of(null, value, tz);
    }

    /**
     * Wrap the given getValue().
     *
     * @param value time in string format (HH:mm:ss)
     * @param tz    time zone, null is treated as {@code UTC}
     * @return object representing the value
     */
    public static ClickHouseTimeValue of(String value, TimeZone tz) {
        return of(null, value, tz);
    }

    /**
     * Update value of the given object or create a new instance if {@code ref} is
     * null.
     *
     * @param ref   object to update, could be null
     * @param value value
     * @param tz    time zone, null is treated as {@code UTC}
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ClickHouseTimeValue of(ClickHouseValue ref, LocalTime value, TimeZone tz) {
        return ref instanceof ClickHouseTimeValue
                ? (ClickHouseTimeValue) ((ClickHouseTimeValue) ref).set(value)
                : new ClickHouseTimeValue(value, tz);
    }

    /**
     * Update value of the given object or create a new instance if {@code ref} is
     * null.
     * 
     * @param ref   object to update, could be null
     * @param value time in string format (HH:mm:ss)
     * @param tz    time zone, null is treated as {@code UTC}
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ClickHouseTimeValue of(ClickHouseValue ref, String value, TimeZone tz) {
        LocalTime time = value == null || value.isEmpty() ? null
                : LocalTime.parse(value, timeFormatter);
        return of(ref, time, tz);
    }

    private final TimeZone tz;

    protected ClickHouseTimeValue(LocalTime value, TimeZone tz) {
        super(value);
        this.tz = tz != null ? tz : ClickHouseValues.UTC_TIMEZONE;
    }

    @Override
    public ClickHouseTimeValue copy(boolean deep) {
        return new ClickHouseTimeValue(getValue(), tz);
    }

    @Override
    public byte asByte() {
        return isNullOrEmpty() ? (byte) 0 : (byte) getValue().toSecondOfDay();
    }

    @Override
    public short asShort() {
        return isNullOrEmpty() ? (short) 0 : (short) getValue().toSecondOfDay();
    }

    @Override
    public int asInteger() {
        return isNullOrEmpty() ? 0 : getValue().toSecondOfDay();
    }

    @Override
    public long asLong() {
        return isNullOrEmpty() ? 0L : getValue().toSecondOfDay();
    }

    @Override
    public float asFloat() {
        return isNullOrEmpty() ? 0F : getValue().toSecondOfDay();
    }

    @Override
    public double asDouble() {
        return isNullOrEmpty() ? 0D : getValue().toSecondOfDay();
    }

    @Override
    public BigInteger asBigInteger() {
        return isNullOrEmpty() ? null : BigInteger.valueOf(getValue().toSecondOfDay());
    }

    @Override
    public BigDecimal asBigDecimal(int scale) {
        return isNullOrEmpty() ? null : new BigDecimal(getValue().toSecondOfDay());
    }

    @Override
    public LocalTime asTime() {
        return getValue();
    }

    @Override
    public Object asObject() {
        return getValue();
    }

    @Override
    public String asString() {
        LocalTime value = getValue();
        return value == null ? null : value.format(timeFormatter);
    }

    @Override
    public ClickHouseTimeValue resetToDefault() {
        set(DEFAULT);
        return this;
    }

    @Override
    public String toSqlExpression() {
        LocalTime value = getValue();
        return value == null ? ClickHouseValues.NULL_EXPR : "'" + value.format(timeFormatter) + "'";
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
        set(value == null ? null : LocalTime.ofSecondOfDay(value.longValue()));
        return this;
    }

    @Override
    public ClickHouseTimeValue update(BigDecimal value) {
        set(value == null ? null : LocalTime.ofSecondOfDay(value.longValue()));
        return this;
    }

    @Override
    public ClickHouseTimeValue update(Enum<?> value) {
        set(value == null ? null : LocalTime.ofSecondOfDay(value.ordinal()));
        return this;
    }

    @Override
    public ClickHouseTimeValue update(LocalTime value) {
        set(value);
        return this;
    }

    @Override
    public ClickHouseTimeValue update(String value) {
        if (value == null || value.isEmpty()) {
            set(null);
        } else {
            set(LocalTime.parse(value, timeFormatter));
        }
        return this;
    }

    @Override
    public ClickHouseTimeValue update(ClickHouseValue value) {
        if (value == null) {
            set(null);
        } else {
            set(value.asTime());
        }
        return this;
    }

    @Override
    public ClickHouseTimeValue update(Object value) {
        if (value == null) {
            set(null);
        } else if (value instanceof LocalTime) {
            set((LocalTime) value);
        } else if (value instanceof Number) {
            set(LocalTime.ofSecondOfDay(((Number) value).longValue()));
        } else {
            set(LocalTime.parse(value.toString(), timeFormatter));
        }
        return this;
    }
} 
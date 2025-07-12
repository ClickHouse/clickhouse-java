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
 * Wrapper class of {@link LocalTime} for Time64 type (64-bit nanoseconds since midnight).
 */
public class ClickHouseTime64Value extends ClickHouseObjectValue<LocalTime> {
    /**
     * Default value.
     */
    public static final LocalTime DEFAULT = LocalTime.ofNanoOfDay(0L);

    static final DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss.nnnnnnnnn");

    /**
     * Create a new instance representing null getValue().
     *
     * @param scale scale (precision)
     * @param tz    time zone, null is treated as {@code UTC}
     * @return new instance representing null value
     */
    public static ClickHouseTime64Value ofNull(int scale, TimeZone tz) {
        return ofNull(null, scale, tz);
    }

    /**
     * Update given value to null or create a new instance if {@code ref} is null.
     * 
     * @param ref   object to update, could be null
     * @param scale scale (precision), only used when {@code ref} is null
     * @param tz    time zone, null is treated as {@code UTC}
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ClickHouseTime64Value ofNull(ClickHouseValue ref, int scale, TimeZone tz) {
        return ref instanceof ClickHouseTime64Value
                ? (ClickHouseTime64Value) ((ClickHouseTime64Value) ref).set(null)
                : new ClickHouseTime64Value(null, scale, tz);
    }

    /**
     * Wrap the given getValue().
     *
     * @param value value
     * @param scale scale (precision)
     * @param tz    time zone, null is treated as {@code UTC}
     * @return object representing the value
     */
    public static ClickHouseTime64Value of(LocalTime value, int scale, TimeZone tz) {
        return of(null, value, scale, tz);
    }

    /**
     * Wrap the given getValue().
     *
     * @param value time in string format (HH:mm:ss.nnnnnnnnn)
     * @param scale scale (precision)
     * @param tz    time zone, null is treated as {@code UTC}
     * @return object representing the value
     */
    public static ClickHouseTime64Value of(String value, int scale, TimeZone tz) {
        return of(null, value, scale, tz);
    }

    /**
     * Update value of the given object or create a new instance if {@code ref} is
     * null.
     *
     * @param ref   object to update, could be null
     * @param value value
     * @param scale scale (precision), only used when {@code ref} is null
     * @param tz    time zone, null is treated as {@code UTC}
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ClickHouseTime64Value of(ClickHouseValue ref, LocalTime value, int scale, TimeZone tz) {
        return ref instanceof ClickHouseTime64Value
                ? (ClickHouseTime64Value) ((ClickHouseTime64Value) ref).set(value)
                : new ClickHouseTime64Value(value, scale, tz);
    }

    /**
     * Update value of the given object or create a new instance if {@code ref} is
     * null.
     * 
     * @param ref   object to update, could be null
     * @param value time in string format (HH:mm:ss.nnnnnnnnn)
     * @param scale scale (precision), only used when {@code ref} is null
     * @param tz    time zone, null is treated as {@code UTC}
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ClickHouseTime64Value of(ClickHouseValue ref, String value, int scale, TimeZone tz) {
        LocalTime time = value == null || value.isEmpty() ? null
                : LocalTime.parse(value, timeFormatter);
        return of(ref, time, scale, tz);
    }

    private final int scale;
    private final TimeZone tz;

    protected ClickHouseTime64Value(LocalTime value, int scale, TimeZone tz) {
        super(value);
        this.scale = ClickHouseChecker.between(scale, ClickHouseValues.PARAM_SCALE, 0, 9);
        this.tz = tz != null ? tz : ClickHouseValues.UTC_TIMEZONE;
    }

    public int getScale() {
        return scale;
    }

    @Override
    public ClickHouseTime64Value copy(boolean deep) {
        return new ClickHouseTime64Value(getValue(), scale, tz);
    }

    @Override
    public byte asByte() {
        return isNullOrEmpty() ? (byte) 0 : (byte) getValue().toNanoOfDay();
    }

    @Override
    public short asShort() {
        return isNullOrEmpty() ? (short) 0 : (short) getValue().toNanoOfDay();
    }

    @Override
    public int asInteger() {
        return isNullOrEmpty() ? 0 : (int) getValue().toNanoOfDay();
    }

    @Override
    public long asLong() {
        return isNullOrEmpty() ? 0L : getValue().toNanoOfDay();
    }

    @Override
    public float asFloat() {
        return isNullOrEmpty() ? 0F : getValue().toNanoOfDay();
    }

    @Override
    public double asDouble() {
        return isNullOrEmpty() ? 0D : getValue().toNanoOfDay();
    }

    @Override
    public BigInteger asBigInteger() {
        return isNullOrEmpty() ? null : BigInteger.valueOf(getValue().toNanoOfDay());
    }

    @Override
    public BigDecimal asBigDecimal(int scale) {
        LocalTime value = getValue();
        BigDecimal v = null;
        if (value != null) {
            long nanoSeconds = value.toNanoOfDay();
            v = new BigDecimal(BigInteger.valueOf(nanoSeconds), scale);
        }
        return v;
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
    public ClickHouseTime64Value resetToDefault() {
        set(DEFAULT);
        return this;
    }

    @Override
    public String toSqlExpression() {
        LocalTime value = getValue();
        return value == null ? ClickHouseValues.NULL_EXPR : "'" + value.format(timeFormatter) + "'";
    }

    @Override
    public ClickHouseTime64Value update(byte value) {
        set(LocalTime.ofNanoOfDay(value));
        return this;
    }

    @Override
    public ClickHouseTime64Value update(short value) {
        set(LocalTime.ofNanoOfDay(value));
        return this;
    }

    @Override
    public ClickHouseTime64Value update(int value) {
        set(LocalTime.ofNanoOfDay(value));
        return this;
    }

    @Override
    public ClickHouseTime64Value update(long value) {
        set(LocalTime.ofNanoOfDay(value));
        return this;
    }

    @Override
    public ClickHouseTime64Value update(float value) {
        set(LocalTime.ofNanoOfDay((long) value));
        return this;
    }

    @Override
    public ClickHouseTime64Value update(double value) {
        set(LocalTime.ofNanoOfDay((long) value));
        return this;
    }

    @Override
    public ClickHouseTime64Value update(BigInteger value) {
        set(value == null ? null : LocalTime.ofNanoOfDay(value.longValue()));
        return this;
    }

    @Override
    public ClickHouseTime64Value update(BigDecimal value) {
        set(value == null ? null : LocalTime.ofNanoOfDay(value.longValue()));
        return this;
    }

    @Override
    public ClickHouseTime64Value update(Enum<?> value) {
        set(value == null ? null : LocalTime.ofNanoOfDay(value.ordinal()));
        return this;
    }

    @Override
    public ClickHouseTime64Value update(LocalTime value) {
        set(value);
        return this;
    }

    @Override
    public ClickHouseTime64Value update(String value) {
        if (value == null || value.isEmpty()) {
            set(null);
        } else {
            set(LocalTime.parse(value, timeFormatter));
        }
        return this;
    }

    @Override
    public ClickHouseTime64Value update(ClickHouseValue value) {
        if (value == null) {
            set(null);
        } else {
            set(value.asTime());
        }
        return this;
    }

    @Override
    public ClickHouseTime64Value update(Object value) {
        if (value == null) {
            set(null);
        } else if (value instanceof LocalTime) {
            set((LocalTime) value);
        } else if (value instanceof Number) {
            set(LocalTime.ofNanoOfDay(((Number) value).longValue()));
        } else {
            set(LocalTime.parse(value.toString(), timeFormatter));
        }
        return this;
    }
} 
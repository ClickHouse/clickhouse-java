package com.clickhouse.data.value;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;
import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseDataConfig;
import com.clickhouse.data.ClickHouseValue;
import com.clickhouse.data.ClickHouseValues;

/**
 * Wrapper class of {@link LocalDateTime}.
 */
@Deprecated
public class ClickHouseDateTimeValue extends ClickHouseObjectValue<LocalDateTime> {
    /**
     * Default value.
     */
    public static final LocalDateTime DEFAULT = ClickHouseInstantValue.DEFAULT.atOffset(ZoneOffset.UTC)
            .toLocalDateTime();

    static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * Create a new instance representing null getValue().
     *
     * @param scale scale
     * @param tz    time zone, null is treated as {@code UTC}
     * @return new instance representing null value
     */
    public static ClickHouseDateTimeValue ofNull(int scale, TimeZone tz) {
        return ofNull(null, scale, tz);
    }

    /**
     * Update given value to null or create a new instance if {@code ref} is null.
     * 
     * @param ref   object to update, could be null
     * @param scale scale, only used when {@code ref} is null
     * @param tz    time zone, null is treated as {@code UTC}
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ClickHouseDateTimeValue ofNull(ClickHouseValue ref, int scale, TimeZone tz) {
        return ref instanceof ClickHouseDateTimeValue
                ? (ClickHouseDateTimeValue) ((ClickHouseDateTimeValue) ref).set(null)
                : new ClickHouseDateTimeValue(null, scale, tz);
    }

    /**
     * Wrap the given getValue().
     *
     * @param value value
     * @param scale scale
     * @param tz    time zone, null is treated as {@code UTC}
     * @return object representing the value
     */
    public static ClickHouseDateTimeValue of(LocalDateTime value, int scale, TimeZone tz) {
        return of(null, value, scale, tz);
    }

    /**
     * Wrap the given getValue().
     *
     * @param value UTC date time in string
     * @param scale scale
     * @param tz    time zone, null is treated as {@code UTC}
     * @return object representing the value
     */
    public static ClickHouseDateTimeValue of(String value, int scale, TimeZone tz) {
        return of(null, value, scale, tz);
    }

    /**
     * Update value of the given object or create a new instance if {@code ref} is
     * null.
     *
     * @param ref   object to update, could be null
     * @param value value
     * @param scale scale, only used when {@code ref} is null
     * @param tz    time zone, null is treated as {@code UTC}
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ClickHouseDateTimeValue of(ClickHouseValue ref, LocalDateTime value, int scale, TimeZone tz) {
        return ref instanceof ClickHouseDateTimeValue
                ? (ClickHouseDateTimeValue) ((ClickHouseDateTimeValue) ref).set(value)
                : new ClickHouseDateTimeValue(value, scale, tz);
    }

    /**
     * Update value of the given object or create a new instance if {@code ref} is
     * null.
     * 
     * @param ref   object to update, could be null
     * @param value UTC date time in string
     * @param scale scale
     * @param tz    time zone, null is treated as {@code UTC}
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ClickHouseDateTimeValue of(ClickHouseValue ref, String value, int scale, TimeZone tz) {
        LocalDateTime dateTime = value == null || value.isEmpty() ? null
                : LocalDateTime.parse(value, ClickHouseValues.DATETIME_FORMATTER);
        return of(ref, dateTime, scale, tz);
    }

    private final int scale;
    private final TimeZone tz;
    private final LocalDateTime defaultValue;

    protected ClickHouseDateTimeValue(LocalDateTime value, int scale, TimeZone tz) {
        super(value);
        this.scale = ClickHouseChecker.between(scale, ClickHouseValues.PARAM_SCALE, 0, 9);
        this.tz = tz != null ? tz : ClickHouseValues.UTC_TIMEZONE;
        this.defaultValue = this.tz.equals(ClickHouseValues.UTC_TIMEZONE) ? DEFAULT
                : ClickHouseOffsetDateTimeValue.DEFAULT.toZonedDateTime().withZoneSameInstant(this.tz.toZoneId())
                        .toLocalDateTime();
    }

    public int getScale() {
        return scale;
    }

    @Override
    public ClickHouseDateTimeValue copy(boolean deep) {
        return new ClickHouseDateTimeValue(getValue(), scale, tz);
    }

    @Override
    public byte asByte() {
        return isNullOrEmpty() ? (byte) 0 : (byte) getValue().atZone(tz.toZoneId()).toEpochSecond();
    }

    @Override
    public short asShort() {
        return isNullOrEmpty() ? (short) 0 : (short) getValue().atZone(tz.toZoneId()).toEpochSecond();
    }

    @Override
    public int asInteger() {
        return isNullOrEmpty() ? 0 : (int) getValue().atZone(tz.toZoneId()).toEpochSecond();
    }

    @Override
    public long asLong() {
        return isNullOrEmpty() ? 0L : getValue().atZone(tz.toZoneId()).toEpochSecond();
    }

    @Override
    public float asFloat() {
        return isNullOrEmpty() ? 0F
                : getValue().atZone(tz.toZoneId()).toEpochSecond()
                        + getValue().getNano() / ClickHouseValues.NANOS.floatValue();
    }

    @Override
    public double asDouble() {
        return isNullOrEmpty() ? 0D
                : getValue().atZone(tz.toZoneId()).toEpochSecond()
                        + getValue().getNano() / ClickHouseValues.NANOS.doubleValue();
    }

    @Override
    public BigInteger asBigInteger() {
        return isNullOrEmpty() ? null : BigInteger.valueOf(getValue().atZone(tz.toZoneId()).toEpochSecond());
    }

    @Override
    public BigDecimal asBigDecimal(int scale) {
        LocalDateTime value = getValue();
        BigDecimal v = null;
        if (value != null) {
            int nanoSeconds = value.getNano();
            v = new BigDecimal(BigInteger.valueOf(value.atZone(tz.toZoneId()).toEpochSecond()), scale);
            if (scale != 0 && nanoSeconds != 0) {
                v = v.add(BigDecimal.valueOf(nanoSeconds).divide(ClickHouseValues.NANOS).setScale(scale,
                        ClickHouseDataConfig.DEFAULT_ROUNDING_MODE));
            }
        }
        return v;
    }

    @Override
    public LocalDate asDate() {
        return isNullOrEmpty() ? null : asDateTime(0).toLocalDate();
    }

    @Override
    public LocalDateTime asDateTime(int scale) {
        return getValue();
    }

    @Override
    public Instant asInstant(int scale) {
        return isNullOrEmpty() ? null : getValue().atZone(tz.toZoneId()).toInstant();
    }

    @Override
    public OffsetDateTime asOffsetDateTime(int scale) {
        if (isNullOrEmpty()) {
            return null;
        }

        return getValue().atZone(tz.toZoneId()).toOffsetDateTime();
    }

    @Override
    public ZonedDateTime asZonedDateTime(int scale) {
        if (isNullOrEmpty()) {
            return null;
        }

        return getValue().atZone(tz.toZoneId());
    }

    @Override
    public Object asObject() {
        return getValue();
    }

    @Override
    public String asString() {
        if (isNullOrEmpty()) {
            return null;
        }

        // different formatter for each scale?
        return getValue().format(scale > 0 ? ClickHouseValues.DATETIME_FORMATTER : dateTimeFormatter);
    }

    @Override
    public ClickHouseDateTimeValue resetToDefault() {
        set(defaultValue);
        return this;
    }

    @Override
    public String toSqlExpression() {
        if (isNullOrEmpty()) {
            return ClickHouseValues.NULL_EXPR;
        }

        return new StringBuilder().append('\'')
                .append(getValue().format(scale > 0 ? ClickHouseValues.DATETIME_FORMATTER : dateTimeFormatter))
                .append('\'').toString();
    }

    @Override
    public ClickHouseDateTimeValue update(byte value) {
        return update(BigInteger.valueOf(value));
    }

    @Override
    public ClickHouseDateTimeValue update(short value) {
        return update(BigInteger.valueOf(value));
    }

    @Override
    public ClickHouseDateTimeValue update(int value) {
        return update(BigInteger.valueOf(value));
    }

    @Override
    public ClickHouseDateTimeValue update(long value) {
        return update(BigInteger.valueOf(value));
    }

    @Override
    public ClickHouseDateTimeValue update(float value) {
        return update(BigDecimal.valueOf(value));
    }

    @Override
    public ClickHouseDateTimeValue update(double value) {
        return update(BigDecimal.valueOf(value));
    }

    @Override
    public ClickHouseDateTimeValue update(BigInteger value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else if (scale == 0) {
            set(ClickHouseValues.convertToDateTime(new BigDecimal(value, 0)));
        } else {
            set(ClickHouseValues.convertToDateTime(new BigDecimal(value, scale)));
        }
        return this;
    }

    @Override
    public ClickHouseDateTimeValue update(BigDecimal value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            if (value.scale() != scale) {
                value = value.setScale(scale, ClickHouseDataConfig.DEFAULT_ROUNDING_MODE);
            }
            set(ClickHouseValues.convertToDateTime(value));
        }
        return this;
    }

    @Override
    public ClickHouseDateTimeValue update(Enum<?> value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            update(BigInteger.valueOf(value.ordinal()));
        }
        return this;
    }

    @Override
    public ClickHouseDateTimeValue update(LocalDate value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(LocalDateTime.of(value, LocalTime.MIN));
        }
        return this;
    }

    @Override
    public ClickHouseDateTimeValue update(LocalTime value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(LocalDateTime.of(LocalDate.now(), value));
        }
        return this;
    }

    @Override
    public ClickHouseDateTimeValue update(LocalDateTime value) {
        set(value);
        return this;
    }

    @Override
    public ClickHouseDateTimeValue update(Instant value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(LocalDateTime.ofInstant(value, tz.toZoneId()));
        }
        return this;
    }

    @Override
    public ClickHouseValue update(OffsetDateTime value) {
        return update(value != null ? value.atZoneSameInstant(tz.toZoneId()).toLocalDateTime() : null);
    }

    @Override
    public ClickHouseValue update(ZonedDateTime value) {
        return update(value != null ? LocalDateTime.ofInstant(value.toInstant(), tz.toZoneId()) : null);
    }

    @Override
    public ClickHouseDateTimeValue update(String value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else if (value.isEmpty()) {
            resetToDefault();
        } else {
            set(LocalDateTime.parse(value, ClickHouseValues.DATETIME_FORMATTER));
        }
        return this;
    }

    @Override
    public ClickHouseDateTimeValue update(ClickHouseValue value) {
        if (value == null || value.isNullOrEmpty()) {
            resetToNullOrEmpty();
        } else {
            set(value.asDateTime(scale));
        }
        return this;
    }

    @Override
    public ClickHouseDateTimeValue update(Object value) {
        if (value instanceof LocalDateTime) {
            set((LocalDateTime) value);
        } else if (value instanceof String) {
            update((String) value);
        } else {
            super.update(value);
        }
        return this;
    }
}

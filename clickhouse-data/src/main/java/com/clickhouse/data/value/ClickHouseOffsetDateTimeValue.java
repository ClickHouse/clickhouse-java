package com.clickhouse.data.value;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.TimeZone;
import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseDataConfig;
import com.clickhouse.data.ClickHouseValue;
import com.clickhouse.data.ClickHouseValues;

/**
 * Wrapper class of {@link OffsetDateTime}.
 */
@Deprecated
public class ClickHouseOffsetDateTimeValue extends ClickHouseObjectValue<OffsetDateTime> {
    /**
     * Default value.
     */
    public static final OffsetDateTime DEFAULT = ClickHouseInstantValue.DEFAULT.atOffset(ZoneOffset.UTC);

    /**
     * Create a new instance representing null getValue().
     *
     * @param scale scale
     * @param tz    time zone, null is treated as {@code UTC}
     * @return new instance representing null value
     */
    public static ClickHouseOffsetDateTimeValue ofNull(int scale, TimeZone tz) {
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
    public static ClickHouseOffsetDateTimeValue ofNull(ClickHouseValue ref, int scale, TimeZone tz) {
        return ref instanceof ClickHouseOffsetDateTimeValue
                ? (ClickHouseOffsetDateTimeValue) ((ClickHouseOffsetDateTimeValue) ref).set(null)
                : new ClickHouseOffsetDateTimeValue(null, scale, tz);
    }

    /**
     * Wrap the given getValue().
     *
     * @param value value
     * @param scale scale
     * @param tz    time zone, null is treated as {@code UTC}
     * @return object representing the value
     */
    public static ClickHouseOffsetDateTimeValue of(LocalDateTime value, int scale, TimeZone tz) {
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
    public static ClickHouseOffsetDateTimeValue of(ClickHouseValue ref, LocalDateTime value, int scale, TimeZone tz) {
        OffsetDateTime v = null;
        if (value != null) {
            v = value.atZone(
                    tz == null || tz.equals(ClickHouseValues.UTC_TIMEZONE) ? ClickHouseValues.UTC_ZONE : tz.toZoneId())
                    .toOffsetDateTime();
        }
        return ref instanceof ClickHouseOffsetDateTimeValue
                ? (ClickHouseOffsetDateTimeValue) ((ClickHouseOffsetDateTimeValue) ref).set(v)
                : new ClickHouseOffsetDateTimeValue(v, scale, tz);
    }

    private final int scale;
    private final TimeZone tz;
    private final OffsetDateTime defaultValue;

    protected ClickHouseOffsetDateTimeValue(OffsetDateTime value, int scale, TimeZone tz) {
        super(value);
        this.scale = ClickHouseChecker.between(scale, ClickHouseValues.PARAM_SCALE, 0, 9);
        if (tz == null || tz.equals(ClickHouseValues.UTC_TIMEZONE)) {
            this.tz = ClickHouseValues.UTC_TIMEZONE;
            this.defaultValue = DEFAULT;
        } else {
            this.tz = tz;
            this.defaultValue = ClickHouseInstantValue.DEFAULT
                    .atOffset(this.tz.toZoneId().getRules().getOffset(ClickHouseInstantValue.DEFAULT));
        }
    }

    public int getScale() {
        return scale;
    }

    @Override
    public ClickHouseOffsetDateTimeValue copy(boolean deep) {
        return new ClickHouseOffsetDateTimeValue(getValue(), scale, tz);
    }

    @Override
    public byte asByte() {
        return isNullOrEmpty() ? (byte) 0 : (byte) getValue().toEpochSecond();
    }

    @Override
    public short asShort() {
        return isNullOrEmpty() ? (short) 0 : (short) getValue().toEpochSecond();
    }

    @Override
    public int asInteger() {
        return isNullOrEmpty() ? 0 : (int) getValue().toEpochSecond();
    }

    @Override
    public long asLong() {
        return isNullOrEmpty() ? 0L : getValue().toEpochSecond();
    }

    @Override
    public float asFloat() {
        return isNullOrEmpty() ? 0F
                : getValue().toEpochSecond() + getValue().getNano() / ClickHouseValues.NANOS.floatValue();
    }

    @Override
    public double asDouble() {
        return isNullOrEmpty() ? 0D
                : getValue().toEpochSecond() + getValue().getNano() / ClickHouseValues.NANOS.doubleValue();
    }

    @Override
    public BigInteger asBigInteger() {
        return isNullOrEmpty() ? null : BigInteger.valueOf(getValue().toEpochSecond());
    }

    @Override
    public BigDecimal asBigDecimal(int scale) {
        OffsetDateTime value = getValue();
        BigDecimal v = null;
        if (value != null) {
            int nanoSeconds = value.getNano();
            v = new BigDecimal(BigInteger.valueOf(value.toEpochSecond()), scale);
            if (scale != 0 && nanoSeconds != 0) {
                v = v.add(BigDecimal.valueOf(nanoSeconds).divide(ClickHouseValues.NANOS).setScale(scale,
                        ClickHouseDataConfig.DEFAULT_ROUNDING_MODE));
            }
        }
        return v;
    }

    @Override
    public LocalDate asDate() {
        return isNullOrEmpty() ? null : asOffsetDateTime(0).toLocalDate();
    }

    @Override
    public LocalTime asTime(int scale) {
        return isNullOrEmpty() ? null : asOffsetDateTime(scale).toLocalTime();
    }

    @Override
    public LocalDateTime asDateTime(int scale) {
        if (isNullOrEmpty()) {
            return null;
        }

        return getValue().toLocalDateTime();
    }

    @Override
    public Instant asInstant(int scale) {
        if (isNullOrEmpty()) {
            return null;
        }

        return getValue().toInstant();
    }

    @Override
    public OffsetDateTime asOffsetDateTime(int scale) {
        return getValue();
    }

    @Override
    public ZonedDateTime asZonedDateTime(int scale) {
        if (isNullOrEmpty()) {
            return null;
        }

        return getValue().toZonedDateTime();
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
        return asDateTime(scale)
                .format(scale > 0 ? ClickHouseValues.DATETIME_FORMATTER : ClickHouseDateTimeValue.dateTimeFormatter);
    }

    @Override
    public ClickHouseOffsetDateTimeValue resetToDefault() {
        set(defaultValue);
        return this;
    }

    @Override
    public String toSqlExpression() {
        if (isNullOrEmpty()) {
            return ClickHouseValues.NULL_EXPR;
        }

        return new StringBuilder().append('\'')
                .append(asDateTime(scale).format(
                        scale > 0 ? ClickHouseValues.DATETIME_FORMATTER : ClickHouseDateTimeValue.dateTimeFormatter))
                .append('\'').toString();
    }

    @Override
    public ClickHouseOffsetDateTimeValue update(byte value) {
        return update(BigInteger.valueOf(value));
    }

    @Override
    public ClickHouseOffsetDateTimeValue update(short value) {
        return update(BigInteger.valueOf(value));
    }

    @Override
    public ClickHouseOffsetDateTimeValue update(int value) {
        return update(BigInteger.valueOf(value));
    }

    @Override
    public ClickHouseOffsetDateTimeValue update(long value) {
        return update(BigInteger.valueOf(value));
    }

    @Override
    public ClickHouseOffsetDateTimeValue update(float value) {
        return update(BigDecimal.valueOf(value));
    }

    @Override
    public ClickHouseOffsetDateTimeValue update(double value) {
        return update(BigDecimal.valueOf(value));
    }

    @Override
    public ClickHouseOffsetDateTimeValue update(BigInteger value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else if (scale == 0) {
            set(ClickHouseValues.convertToDateTime(new BigDecimal(value, 0), tz).toOffsetDateTime());
        } else {
            set(ClickHouseValues.convertToDateTime(new BigDecimal(value, scale), tz).toOffsetDateTime());
        }
        return this;
    }

    @Override
    public ClickHouseOffsetDateTimeValue update(BigDecimal value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            if (value.scale() != scale) {
                value = value.setScale(scale, ClickHouseDataConfig.DEFAULT_ROUNDING_MODE);
            }
            set(ClickHouseValues.convertToDateTime(value, tz).toOffsetDateTime());
        }
        return this;
    }

    @Override
    public ClickHouseOffsetDateTimeValue update(Enum<?> value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            update(BigInteger.valueOf(value.ordinal()));
        }
        return this;
    }

    @Override
    public ClickHouseOffsetDateTimeValue update(LocalDate value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            LocalDateTime dateTime = LocalDateTime.of(value, LocalTime.MIN);
            set(tz != null ? dateTime.atZone(tz.toZoneId()).toOffsetDateTime() : dateTime.atOffset(ZoneOffset.UTC));
        }
        return this;
    }

    @Override
    public ClickHouseOffsetDateTimeValue update(LocalTime value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            LocalDateTime dateTime = LocalDateTime.of(LocalDate.now(), value);
            set(tz != null ? dateTime.atZone(tz.toZoneId()).toOffsetDateTime() : dateTime.atOffset(ZoneOffset.UTC));
        }
        return this;
    }

    @Override
    public ClickHouseOffsetDateTimeValue update(LocalDateTime value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(tz != null && !tz.equals(ClickHouseValues.UTC_TIMEZONE) ? value.atZone(tz.toZoneId()).toOffsetDateTime()
                    : value.atOffset(ZoneOffset.UTC));
        }
        return this;
    }

    @Override
    public ClickHouseOffsetDateTimeValue update(Instant value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(OffsetDateTime.ofInstant(value, tz.toZoneId()));
        }
        return this;
    }

    @Override
    public ClickHouseOffsetDateTimeValue update(OffsetDateTime value) {
        set(value);
        return this;
    }

    @Override
    public ClickHouseOffsetDateTimeValue update(ZonedDateTime value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(value.toOffsetDateTime());
        }
        return this;
    }

    @Override
    public ClickHouseOffsetDateTimeValue update(String value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else if (value.isEmpty()) {
            resetToDefault();
        } else {
            set(LocalDateTime.parse(value, ClickHouseValues.DATETIME_FORMATTER).atZone(tz.toZoneId())
                    .toOffsetDateTime());
        }
        return this;
    }

    @Override
    public ClickHouseOffsetDateTimeValue update(ClickHouseValue value) {
        if (value == null || value.isNullOrEmpty()) {
            resetToNullOrEmpty();
        } else {
            set(value.asOffsetDateTime(scale));
        }
        return this;
    }

    @Override
    public ClickHouseOffsetDateTimeValue update(Object value) {
        if (value instanceof OffsetDateTime) {
            set((OffsetDateTime) value);
        } else if (value instanceof String) {
            update((String) value);
        } else {
            super.update(value);
        }
        return this;
    }
}

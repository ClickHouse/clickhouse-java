package com.clickhouse.data.value;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.TimeZone;
import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseDataConfig;
import com.clickhouse.data.ClickHouseValue;
import com.clickhouse.data.ClickHouseValues;

/**
 * Wrapper class of {@link Instant}.
 */
@Deprecated
public class ClickHouseInstantValue extends ClickHouseObjectValue<Instant> {
    /**
     * Default instant.
     */
    public static final Instant DEFAULT = Instant.ofEpochMilli(0L);

    /**
     * Create a new instance representing null getValue().
     *
     * @param scale scale
     * @param tz    time zone, null is treated as {@code UTC}
     * @return new instance representing null value
     */
    public static ClickHouseInstantValue ofNull(int scale, TimeZone tz) {
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
    public static ClickHouseInstantValue ofNull(ClickHouseValue ref, int scale, TimeZone tz) {
        return ref instanceof ClickHouseInstantValue
                ? (ClickHouseInstantValue) ((ClickHouseInstantValue) ref).set(null)
                : new ClickHouseInstantValue(null, scale, tz);
    }

    /**
     * Wrap the given getValue().
     *
     * @param value value
     * @param scale scale
     * @param tz    time zone, null is treated as {@code UTC}
     * @return object representing the value
     */
    public static ClickHouseInstantValue of(Instant value, int scale, TimeZone tz) {
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
    public static ClickHouseInstantValue of(String value, int scale, TimeZone tz) {
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
    public static ClickHouseInstantValue of(ClickHouseValue ref, Instant value, int scale, TimeZone tz) {
        return ref instanceof ClickHouseInstantValue
                ? (ClickHouseInstantValue) ((ClickHouseInstantValue) ref).set(value)
                : new ClickHouseInstantValue(value, scale, tz);
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
    public static ClickHouseInstantValue of(ClickHouseValue ref, String value, int scale, TimeZone tz) {
        Instant dateTime = value == null || value.isEmpty() ? null
                : LocalDateTime.parse(value, ClickHouseValues.DATETIME_FORMATTER).atZone(tz.toZoneId()).toInstant();
        return of(ref, dateTime, scale, tz);
    }

    private final int scale;
    private final TimeZone tz;

    protected ClickHouseInstantValue(Instant value, int scale, TimeZone tz) {
        super(value);
        this.scale = ClickHouseChecker.between(scale, ClickHouseValues.PARAM_SCALE, 0, 9);
        this.tz = tz != null ? tz : ClickHouseValues.UTC_TIMEZONE;
    }

    public int getScale() {
        return scale;
    }

    @Override
    public ClickHouseInstantValue copy(boolean deep) {
        return new ClickHouseInstantValue(getValue(), scale, tz);
    }

    @Override
    public byte asByte() {
        return isNullOrEmpty() ? (byte) 0 : (byte) getValue().getEpochSecond();
    }

    @Override
    public short asShort() {
        return isNullOrEmpty() ? (short) 0 : (short) getValue().getEpochSecond();
    }

    @Override
    public int asInteger() {
        return isNullOrEmpty() ? 0 : (int) getValue().getEpochSecond();
    }

    @Override
    public long asLong() {
        return isNullOrEmpty() ? 0L : getValue().getEpochSecond();
    }

    @Override
    public float asFloat() {
        return isNullOrEmpty() ? 0F
                : getValue().getEpochSecond() + getValue().getNano() / ClickHouseValues.NANOS.floatValue();
    }

    @Override
    public double asDouble() {
        return isNullOrEmpty() ? 0D
                : getValue().getEpochSecond()
                        + getValue().getNano() / ClickHouseValues.NANOS.doubleValue();
    }

    @Override
    public BigInteger asBigInteger() {
        return isNullOrEmpty() ? null : BigInteger.valueOf(getValue().getEpochSecond());
    }

    @Override
    public BigDecimal asBigDecimal(int scale) {
        Instant value = getValue();
        BigDecimal v = null;
        if (value != null) {
            int nanoSeconds = value.getNano();
            v = new BigDecimal(BigInteger.valueOf(value.getEpochSecond()), scale);
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
        if (isNullOrEmpty()) {
            return null;
        }

        return LocalDateTime.ofInstant(getValue(), tz.toZoneId());
    }

    @Override
    public Instant asInstant(int scale) {
        return getValue();
    }

    @Override
    public OffsetDateTime asOffsetDateTime(int scale) {
        if (isNullOrEmpty()) {
            return null;
        }

        return OffsetDateTime.ofInstant(getValue(), tz.toZoneId());
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
        return asDateTime()
                .format(scale > 0 ? ClickHouseValues.DATETIME_FORMATTER : ClickHouseDateTimeValue.dateTimeFormatter);
    }

    @Override
    public ClickHouseInstantValue resetToDefault() {
        set(DEFAULT);
        return this;
    }

    @Override
    public String toSqlExpression() {
        if (isNullOrEmpty()) {
            return ClickHouseValues.NULL_EXPR;
        }

        return new StringBuilder().append('\'')
                .append(asDateTime().format(
                        scale > 0 ? ClickHouseValues.DATETIME_FORMATTER : ClickHouseDateTimeValue.dateTimeFormatter))
                .append('\'').toString();
    }

    @Override
    public ClickHouseInstantValue update(byte value) {
        return update(BigInteger.valueOf(value));
    }

    @Override
    public ClickHouseInstantValue update(short value) {
        return update(BigInteger.valueOf(value));
    }

    @Override
    public ClickHouseInstantValue update(int value) {
        return update(BigInteger.valueOf(value));
    }

    @Override
    public ClickHouseInstantValue update(long value) {
        return update(BigInteger.valueOf(value));
    }

    @Override
    public ClickHouseInstantValue update(float value) {
        return update(BigDecimal.valueOf(value));
    }

    @Override
    public ClickHouseInstantValue update(double value) {
        return update(BigDecimal.valueOf(value));
    }

    @Override
    public ClickHouseInstantValue update(BigInteger value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else if (scale == 0) {
            set(ClickHouseValues.convertToInstant(new BigDecimal(value, 0)));
        } else {
            set(ClickHouseValues.convertToInstant(new BigDecimal(value, scale)));
        }
        return this;
    }

    @Override
    public ClickHouseInstantValue update(BigDecimal value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            if (value.scale() != scale) {
                value = value.setScale(scale, ClickHouseDataConfig.DEFAULT_ROUNDING_MODE);
            }
            set(ClickHouseValues.convertToInstant(value));
        }
        return this;
    }

    @Override
    public ClickHouseInstantValue update(Enum<?> value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            update(BigInteger.valueOf(value.ordinal()));
        }
        return this;
    }

    @Override
    public ClickHouseInstantValue update(LocalDate value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(LocalDateTime.of(value, LocalTime.MIN).atZone(tz.toZoneId()).toInstant());
        }
        return this;
    }

    @Override
    public ClickHouseInstantValue update(LocalTime value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(LocalDateTime.of(LocalDate.now(), value).atZone(tz.toZoneId()).toInstant());
        }
        return this;
    }

    @Override
    public ClickHouseInstantValue update(LocalDateTime value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(value.atZone(tz.toZoneId()).toInstant());
        }
        return this;
    }

    @Override
    public ClickHouseInstantValue update(Instant value) {
        set(value);
        return this;
    }

    @Override
    public ClickHouseInstantValue update(OffsetDateTime value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(value.toInstant());
        }
        return this;
    }

    @Override
    public ClickHouseInstantValue update(ZonedDateTime value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(value.toInstant());
        }
        return this;
    }

    @Override
    public ClickHouseInstantValue update(String value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else if (value.isEmpty()) {
            resetToDefault();
        } else {
            set(LocalDateTime.parse(value, ClickHouseValues.DATETIME_FORMATTER).atZone(tz.toZoneId()).toInstant());
        }
        return this;
    }

    @Override
    public ClickHouseInstantValue update(ClickHouseValue value) {
        if (value == null || value.isNullOrEmpty()) {
            resetToNullOrEmpty();
        } else {
            set(value.asInstant(scale));
        }
        return this;
    }

    @Override
    public ClickHouseInstantValue update(Object value) {
        if (value instanceof Instant) {
            update((Instant) value);
        } else if (value instanceof String) {
            update((String) value);
        } else {
            super.update(value);
        }
        return this;
    }
}

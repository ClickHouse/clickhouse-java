package com.clickhouse.client.data;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import com.clickhouse.client.ClickHouseChecker;
import com.clickhouse.client.ClickHouseValue;
import com.clickhouse.client.ClickHouseValues;

/**
 * Wraper class of LocalDateTime.
 */
public class ClickHouseDateTimeValue extends ClickHouseObjectValue<LocalDateTime> {
    static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * Create a new instance representing null getValue().
     *
     * @param scale scale
     * @return new instance representing null value
     */
    public static ClickHouseDateTimeValue ofNull(int scale) {
        return ofNull(null, scale);
    }

    /**
     * Update given value to null or create a new instance if {@code ref} is null.
     * 
     * @param ref   object to update, could be null
     * @param scale scale, only used when {@code ref} is null
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ClickHouseDateTimeValue ofNull(ClickHouseValue ref, int scale) {
        return ref instanceof ClickHouseDateTimeValue
                ? (ClickHouseDateTimeValue) ((ClickHouseDateTimeValue) ref).set(null)
                : new ClickHouseDateTimeValue(null, scale);
    }

    /**
     * Wrap the given getValue().
     *
     * @param value value
     * @param scale scale
     * @return object representing the value
     */
    public static ClickHouseDateTimeValue of(LocalDateTime value, int scale) {
        return of(null, value, scale);
    }

    /**
     * Wrap the given getValue().
     *
     * @param value UTC date time in string
     * @param scale scale
     * @return object representing the value
     */
    public static ClickHouseDateTimeValue of(String value, int scale) {
        return of(null, value, scale);
    }

    /**
     * Update value of the given object or create a new instance if {@code ref} is
     * null.
     *
     * @param ref   object to update, could be null
     * @param value value
     * @param scale scale, only used when {@code ref} is null
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ClickHouseDateTimeValue of(ClickHouseValue ref, LocalDateTime value, int scale) {
        return ref instanceof ClickHouseDateTimeValue
                ? (ClickHouseDateTimeValue) ((ClickHouseDateTimeValue) ref).set(value)
                : new ClickHouseDateTimeValue(value, scale);
    }

    /**
     * Update value of the given object or create a new instance if {@code ref} is
     * null.
     * 
     * @param ref   object to update, could be null
     * @param value UTC date time in string
     * @param scale scale
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ClickHouseDateTimeValue of(ClickHouseValue ref, String value, int scale) {
        LocalDateTime dateTime = value == null || value.isEmpty() ? null
                : LocalDateTime.parse(value, ClickHouseValues.DATETIME_FORMATTER);
        return of(ref, dateTime, scale);
    }

    private final int scale;

    protected ClickHouseDateTimeValue(LocalDateTime value, int scale) {
        super(value);
        this.scale = ClickHouseChecker.between(scale, ClickHouseValues.PARAM_SCALE, 0, 9);
    }

    public int getScale() {
        return scale;
    }

    @Override
    public ClickHouseDateTimeValue copy(boolean deep) {
        return new ClickHouseDateTimeValue(getValue(), scale);
    }

    @Override
    public byte asByte() {
        return isNullOrEmpty() ? (byte) 0 : (byte) getValue().toEpochSecond(ZoneOffset.UTC);
    }

    @Override
    public short asShort() {
        return isNullOrEmpty() ? (short) 0 : (short) getValue().toEpochSecond(ZoneOffset.UTC);
    }

    @Override
    public int asInteger() {
        return isNullOrEmpty() ? 0 : (int) getValue().toEpochSecond(ZoneOffset.UTC);
    }

    @Override
    public long asLong() {
        return isNullOrEmpty() ? 0L : getValue().toEpochSecond(ZoneOffset.UTC);
    }

    @Override
    public float asFloat() {
        return isNullOrEmpty() ? 0F
                : getValue().toEpochSecond(ZoneOffset.UTC) + getValue().getNano() / ClickHouseValues.NANOS.floatValue();
    }

    @Override
    public double asDouble() {
        return isNullOrEmpty() ? 0D
                : getValue().toEpochSecond(ZoneOffset.UTC)
                        + getValue().getNano() / ClickHouseValues.NANOS.doubleValue();
    }

    @Override
    public BigInteger asBigInteger() {
        return isNullOrEmpty() ? null : BigInteger.valueOf(getValue().toEpochSecond(ZoneOffset.UTC));
    }

    @Override
    public BigDecimal asBigDecimal(int scale) {
        LocalDateTime value = getValue();
        BigDecimal v = null;
        if (value != null) {
            int nanoSeconds = value.getNano();
            v = new BigDecimal(BigInteger.valueOf(value.toEpochSecond(ZoneOffset.UTC)), scale);
            if (scale != 0 && nanoSeconds != 0) {
                v = v.add(BigDecimal.valueOf(nanoSeconds).divide(ClickHouseValues.NANOS).setScale(scale,
                        RoundingMode.HALF_UP));
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
    public Object asObject() {
        return getValue();
    }

    @Override
    public String asString(int length, Charset charset) {
        if (isNullOrEmpty()) {
            return null;
        }

        // different formatter for each scale?
        String str = getValue().format(scale > 0 ? ClickHouseValues.DATETIME_FORMATTER : dateTimeFormatter);
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
                value = value.setScale(scale, RoundingMode.HALF_UP);
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
    public ClickHouseDateTimeValue update(String value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(LocalDateTime.parse(value, ClickHouseValues.DATETIME_FORMATTER));
        }
        return this;
    }

    @Override
    public ClickHouseDateTimeValue update(ClickHouseValue value) {
        if (value == null) {
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

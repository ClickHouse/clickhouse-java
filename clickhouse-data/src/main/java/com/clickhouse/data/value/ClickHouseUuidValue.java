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
 * Wrapper class of {@link UUID}.
 */
@Deprecated
public class ClickHouseUuidValue extends ClickHouseObjectValue<UUID> {
    /**
     * Default value.
     */
    public static final UUID DEFAULT = new UUID(0L, 0L);

    /**
     * Create a new instance representing null value.
     *
     * @return new instance representing null value
     */
    public static ClickHouseUuidValue ofNull() {
        return ofNull(null);
    }

    /**
     * Update given value to null or create a new instance if {@code ref} is null.
     * 
     * @param ref object to update, could be null
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ClickHouseUuidValue ofNull(ClickHouseValue ref) {
        return ref instanceof ClickHouseUuidValue ? (ClickHouseUuidValue) ((ClickHouseUuidValue) ref).set(null)
                : new ClickHouseUuidValue(null);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ClickHouseUuidValue of(UUID value) {
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
    public static ClickHouseUuidValue of(ClickHouseValue ref, UUID value) {
        return ref instanceof ClickHouseUuidValue ? (ClickHouseUuidValue) ((ClickHouseUuidValue) ref).set(value)
                : new ClickHouseUuidValue(value);
    }

    protected ClickHouseUuidValue(UUID value) {
        super(value);
    }

    @Override
    public ClickHouseUuidValue copy(boolean deep) {
        return new ClickHouseUuidValue(getValue());
    }

    @Override
    public byte asByte() {
        return isNullOrEmpty() ? (byte) 0 : asBigInteger().byteValue();
    }

    @Override
    public short asShort() {
        return isNullOrEmpty() ? (short) 0 : asBigInteger().shortValue();
    }

    @Override
    public int asInteger() {
        return isNullOrEmpty() ? 0 : asBigInteger().intValue();
    }

    @Override
    public long asLong() {
        return isNullOrEmpty() ? 0L : asBigInteger().longValue();
    }

    @Override
    public BigInteger asBigInteger() {
        return ClickHouseValues.convertToBigInteger(getValue());
    }

    @Override
    public float asFloat() {
        return isNullOrEmpty() ? 0F : asBigInteger().floatValue();
    }

    @Override
    public double asDouble() {
        return isNullOrEmpty() ? 0D : asBigInteger().doubleValue();
    }

    @Override
    public BigDecimal asBigDecimal(int scale) {
        return isNullOrEmpty() ? null : new BigDecimal(asBigInteger(), scale);
    }

    @Override
    public Object asObject() {
        return getValue();
    }

    @Override
    public UUID asUuid() {
        return getValue();
    }

    @Override
    public ClickHouseUuidValue resetToDefault() {
        set(DEFAULT);
        return this;
    }

    @Override
    public String toSqlExpression() {
        if (isNullOrEmpty()) {
            return ClickHouseValues.NULL_EXPR;
        }
        return new StringBuilder().append('\'').append(getValue().toString()).append('\'').toString();
    }

    @Override
    public ClickHouseUuidValue update(byte value) {
        return update(BigInteger.valueOf(value));
    }

    @Override
    public ClickHouseUuidValue update(short value) {
        return update(BigInteger.valueOf(value));
    }

    @Override
    public ClickHouseUuidValue update(int value) {
        return update(BigInteger.valueOf(value));
    }

    @Override
    public ClickHouseUuidValue update(long value) {
        return update(BigInteger.valueOf(value));
    }

    @Override
    public ClickHouseUuidValue update(float value) {
        return update(BigDecimal.valueOf(value).toBigInteger());
    }

    @Override
    public ClickHouseUuidValue update(double value) {
        return update(BigDecimal.valueOf(value).toBigInteger());
    }

    @Override
    public ClickHouseUuidValue update(BigInteger value) {
        set(ClickHouseValues.convertToUuid(value));
        return this;
    }

    @Override
    public ClickHouseUuidValue update(BigDecimal value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            update(value.toBigIntegerExact());
        }
        return this;
    }

    @Override
    public ClickHouseUuidValue update(Enum<?> value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            update(BigInteger.valueOf(value.ordinal()));
        }
        return this;
    }

    @Override
    public ClickHouseUuidValue update(Inet4Address value) {
        return update(ClickHouseValues.convertToBigInteger(value));
    }

    @Override
    public ClickHouseUuidValue update(Inet6Address value) {
        return update(ClickHouseValues.convertToBigInteger(value));
    }

    @Override
    public ClickHouseUuidValue update(LocalDate value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            update(BigInteger.valueOf(value.toEpochDay()));
        }
        return this;
    }

    @Override
    public ClickHouseUuidValue update(LocalTime value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            update(BigInteger.valueOf(value.toSecondOfDay()));
        }
        return this;
    }

    @Override
    public ClickHouseUuidValue update(LocalDateTime value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            update(BigInteger.valueOf(value.toEpochSecond(ZoneOffset.UTC)));
        }
        return this;
    }

    @Override
    public ClickHouseUuidValue update(String value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else if (value.isEmpty()) {
            resetToDefault();
        } else {
            set(UUID.fromString(value));
        }
        return this;
    }

    @Override
    public ClickHouseUuidValue update(UUID value) {
        set(value);
        return this;
    }

    @Override
    public ClickHouseUuidValue update(ClickHouseValue value) {
        if (value == null || value.isNullOrEmpty()) {
            resetToNullOrEmpty();
        } else {
            set(value.asUuid());
        }
        return this;
    }

    @Override
    public ClickHouseUuidValue update(Object value) {
        if (value instanceof UUID) {
            set((UUID) value);
        } else {
            super.update(value);
        }
        return this;
    }
}

package com.clickhouse.data.value;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.UUID;
import com.clickhouse.data.ClickHouseValue;
import com.clickhouse.data.ClickHouseValues;

/**
 * Wrapper class of {@link Inet4Address}.
 */
@Deprecated
public class ClickHouseIpv4Value extends ClickHouseObjectValue<Inet4Address> {
    public static final Inet4Address DEFAULT;

    static {
        try {
            DEFAULT = (Inet4Address) InetAddress.getByAddress("0.0.0.0", new byte[4]);
        } catch (UnknownHostException e) {
            // should not happen
            throw new IllegalStateException("Failed to create default Ipv4 value", e);
        }
    }

    /**
     * Create a new instance representing null value.
     *
     * @return new instance representing null value
     */
    public static ClickHouseIpv4Value ofNull() {
        return ofNull(null);
    }

    /**
     * Update given value to null or create a new instance if {@code ref} is null.
     * 
     * @param ref object to update, could be null
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ClickHouseIpv4Value ofNull(ClickHouseValue ref) {
        return ref instanceof ClickHouseIpv4Value ? (ClickHouseIpv4Value) ((ClickHouseIpv4Value) ref).set(null)
                : new ClickHouseIpv4Value(null);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ClickHouseIpv4Value of(Inet4Address value) {
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
    public static ClickHouseIpv4Value of(ClickHouseValue ref, Inet4Address value) {
        return ref instanceof ClickHouseIpv4Value ? (ClickHouseIpv4Value) ((ClickHouseIpv4Value) ref).set(value)
                : new ClickHouseIpv4Value(value);
    }

    protected ClickHouseIpv4Value(Inet4Address value) {
        super(value);
    }

    @Override
    public ClickHouseIpv4Value copy(boolean deep) {
        return new ClickHouseIpv4Value(getValue());
    }

    @Override
    public byte asByte() {
        BigInteger bigInt = asBigInteger();
        return bigInt == null ? (byte) 0 : bigInt.byteValue();
    }

    @Override
    public short asShort() {
        BigInteger bigInt = asBigInteger();
        return bigInt == null ? (short) 0 : bigInt.shortValue();
    }

    @Override
    public int asInteger() {
        BigInteger bigInt = asBigInteger();
        return bigInt == null ? 0 : bigInt.intValue();
    }

    @Override
    public long asLong() {
        BigInteger bigInt = asBigInteger();
        return bigInt == null ? 0L : bigInt.longValue();
    }

    @Override
    public BigInteger asBigInteger() {
        return isNullOrEmpty() ? null : new BigInteger(1, getValue().getAddress());
    }

    @Override
    public float asFloat() {
        BigInteger bigInt = asBigInteger();
        return bigInt == null ? 0F : bigInt.floatValue();
    }

    @Override
    public double asDouble() {
        BigInteger bigInt = asBigInteger();
        return bigInt == null ? 0D : bigInt.doubleValue();
    }

    @Override
    public BigDecimal asBigDecimal(int scale) {
        BigInteger bigInt = asBigInteger();
        return bigInt == null ? null : new BigDecimal(bigInt, scale);
    }

    @Override
    public Inet4Address asInet4Address() {
        return getValue();
    }

    @Override
    public Inet6Address asInet6Address() {
        return ClickHouseValues.convertToIpv6(getValue());
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

        return String.valueOf(getValue().getHostAddress());
    }

    @Override
    public ClickHouseIpv4Value resetToDefault() {
        set(DEFAULT);
        return this;
    }

    @Override
    public String toSqlExpression() {
        if (isNullOrEmpty()) {
            return ClickHouseValues.NULL_EXPR;
        }
        return new StringBuilder().append('\'').append(getValue().getHostAddress()).append('\'').toString();
    }

    @Override
    public ClickHouseIpv4Value update(byte value) {
        return update((int) value);
    }

    @Override
    public ClickHouseIpv4Value update(short value) {
        return update((int) value);
    }

    @Override
    public ClickHouseIpv4Value update(int value) {
        set(ClickHouseValues.convertToIpv4(value));
        return this;
    }

    @Override
    public ClickHouseIpv4Value update(long value) {
        return update((int) value);
    }

    @Override
    public ClickHouseIpv4Value update(float value) {
        return update((int) value);
    }

    @Override
    public ClickHouseIpv4Value update(double value) {
        return update((int) value);
    }

    @Override
    public ClickHouseIpv4Value update(BigInteger value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            update(value.intValue());
        }
        return this;
    }

    @Override
    public ClickHouseIpv4Value update(BigDecimal value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            update(value.intValue());
        }
        return this;
    }

    @Override
    public ClickHouseIpv4Value update(Enum<?> value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            update(value.ordinal());
        }
        return this;
    }

    @Override
    public ClickHouseIpv4Value update(Inet4Address value) {
        set(value);
        return this;
    }

    @Override
    public ClickHouseIpv4Value update(Inet6Address value) {
        set(ClickHouseValues.convertToIpv4(value));
        return this;
    }

    @Override
    public ClickHouseIpv4Value update(LocalDate value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            update((int) value.toEpochDay());
        }
        return this;
    }

    @Override
    public ClickHouseIpv4Value update(LocalTime value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            update(value.toSecondOfDay());
        }
        return this;
    }

    @Override
    public ClickHouseIpv4Value update(LocalDateTime value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            update((int) value.toEpochSecond(ZoneOffset.UTC));
        }
        return this;
    }

    @Override
    public ClickHouseIpv4Value update(String value) {
        set(ClickHouseValues.convertToIpv4(value));
        return this;
    }

    @Override
    public ClickHouseIpv4Value update(UUID value) {
        BigInteger v = ClickHouseValues.convertToBigInteger(value);
        if (v == null) {
            resetToNullOrEmpty();
        } else {
            update(v.intValue());
        }
        return this;
    }

    @Override
    public ClickHouseIpv4Value update(ClickHouseValue value) {
        if (value == null || value.isNullOrEmpty()) {
            resetToNullOrEmpty();
        } else {
            set(value.asInet4Address());
        }
        return this;
    }

    @Override
    public ClickHouseIpv4Value update(Object value) {
        if (value instanceof Inet4Address) {
            set((Inet4Address) value);
        } else if (value instanceof Inet6Address) {
            set(ClickHouseValues.convertToIpv4((Inet6Address) value));
        } else {
            super.update(value);
        }
        return this;
    }
}

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
 * Wrapper class of {@link Inet6Address}.
 */
@Deprecated
public class ClickHouseIpv6Value extends ClickHouseObjectValue<Inet6Address> {
    public static final Inet6Address DEFAULT;

    static {
        try {
            DEFAULT = (Inet6Address) InetAddress.getByAddress("::0", new byte[16]);
        } catch (UnknownHostException e) {
            // should not happen
            throw new IllegalStateException("Failed to create default Ipv6 value", e);
        }
    }

    /**
     * Create a new instance representing null value.
     *
     * @return new instance representing null value
     */
    public static ClickHouseIpv6Value ofNull() {
        return ofNull(null);
    }

    /**
     * Update given value to null or create a new instance if {@code ref} is null.
     * 
     * @param ref object to update, could be null
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ClickHouseIpv6Value ofNull(ClickHouseValue ref) {
        return ref instanceof ClickHouseIpv6Value ? (ClickHouseIpv6Value) ((ClickHouseIpv6Value) ref).set(null)
                : new ClickHouseIpv6Value(null);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ClickHouseIpv6Value of(Inet6Address value) {
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
    public static ClickHouseIpv6Value of(ClickHouseValue ref, Inet6Address value) {
        return ref instanceof ClickHouseIpv6Value ? (ClickHouseIpv6Value) ((ClickHouseIpv6Value) ref).set(value)
                : new ClickHouseIpv6Value(value);
    }

    protected ClickHouseIpv6Value(Inet6Address value) {
        super(value);
    }

    @Override
    public ClickHouseIpv6Value copy(boolean deep) {
        return new ClickHouseIpv6Value(getValue());
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
        return ClickHouseValues.convertToIpv4(getValue());
    }

    @Override
    public Inet6Address asInet6Address() {
        return getValue();
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
    public ClickHouseIpv6Value resetToDefault() {
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
    public ClickHouseIpv6Value update(byte value) {
        return update(BigInteger.valueOf(value));
    }

    @Override
    public ClickHouseIpv6Value update(short value) {
        return update(BigInteger.valueOf(value));
    }

    @Override
    public ClickHouseIpv6Value update(int value) {
        return update(BigInteger.valueOf(value));
    }

    @Override
    public ClickHouseIpv6Value update(long value) {
        return update(BigInteger.valueOf(value));
    }

    @Override
    public ClickHouseIpv6Value update(float value) {
        return update(BigDecimal.valueOf(value).toBigInteger());
    }

    @Override
    public ClickHouseIpv6Value update(double value) {
        return update(BigDecimal.valueOf(value).toBigInteger());
    }

    @Override
    public ClickHouseIpv6Value update(BigInteger value) {
        set(ClickHouseValues.convertToIpv6(value));
        return this;
    }

    @Override
    public ClickHouseIpv6Value update(BigDecimal value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            update(value.toBigIntegerExact());
        }
        return this;
    }

    @Override
    public ClickHouseIpv6Value update(Enum<?> value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            update(BigInteger.valueOf(value.ordinal()));
        }
        return this;
    }

    @Override
    public ClickHouseIpv6Value update(Inet4Address value) {
        set(ClickHouseValues.convertToIpv6(value));
        return this;
    }

    @Override
    public ClickHouseIpv6Value update(Inet6Address value) {
        set(value);
        return this;
    }

    @Override
    public ClickHouseIpv6Value update(LocalDate value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            update(BigInteger.valueOf(value.toEpochDay()));
        }
        return this;
    }

    @Override
    public ClickHouseIpv6Value update(LocalTime value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            update(BigInteger.valueOf(value.toSecondOfDay()));
        }
        return this;
    }

    @Override
    public ClickHouseIpv6Value update(LocalDateTime value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            update(BigInteger.valueOf(value.toEpochSecond(ZoneOffset.UTC)));
        }
        return this;
    }

    @Override
    public ClickHouseIpv6Value update(String value) {
        set(ClickHouseValues.convertToIpv6(value));
        return this;
    }

    @Override
    public ClickHouseIpv6Value update(UUID value) {
        BigInteger v = ClickHouseValues.convertToBigInteger(value);
        if (v == null) {
            resetToNullOrEmpty();
        } else {
            update(v);
        }
        return this;
    }

    @Override
    public ClickHouseIpv6Value update(ClickHouseValue value) {
        if (value == null || value.isNullOrEmpty()) {
            resetToNullOrEmpty();
        } else {
            set(value.asInet6Address());
        }
        return this;
    }

    @Override
    public ClickHouseIpv6Value update(Object value) {
        if (value instanceof Inet6Address) {
            set((Inet6Address) value);
        } else if (value instanceof Inet4Address) {
            set(ClickHouseValues.convertToIpv6((Inet4Address) value));
        } else {
            super.update(value);
        }
        return this;
    }
}

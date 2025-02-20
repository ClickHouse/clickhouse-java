package com.clickhouse.data.value;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

import com.clickhouse.data.ClickHouseUtils;
import com.clickhouse.data.ClickHouseValue;
import com.clickhouse.data.ClickHouseValues;

/**
 * Wrapper class of {@link String}.
 */
@Deprecated
public class ClickHouseStringValue implements ClickHouseValue {
    /**
     * Create a new instance representing null value.
     *
     * @return new instance representing null value
     */
    public static ClickHouseStringValue ofNull() {
        return ofNull(null);
    }

    /**
     * Update given value to null or create a new instance if {@code ref} is null.
     * 
     * @param ref object to update, could be null
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ClickHouseStringValue ofNull(ClickHouseValue ref) {
        return ref instanceof ClickHouseStringValue ? ((ClickHouseStringValue) ref).set((String) null)
                : new ClickHouseStringValue((String) null);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ClickHouseStringValue of(String value) {
        return of(null, value);
    }

    /**
     * Wrap the given value.
     *
     * @param bytes bytes
     * @return object representing the value
     */
    public static ClickHouseStringValue of(byte[] bytes) {
        return of(null, bytes);
    }

    /**
     * Update value of the given object or create a new instance if {@code ref} is
     * null.
     *
     * @param ref   object to update, could be null
     * @param value value
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ClickHouseStringValue of(ClickHouseValue ref, String value) {
        return ref instanceof ClickHouseStringValue ? ((ClickHouseStringValue) ref).set(value)
                : new ClickHouseStringValue(value);
    }

    /**
     * Update value of the given object or create a new instance if {@code ref} is
     * null.
     *
     * @param ref   object to update, could be null
     * @param bytes bytes
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ClickHouseStringValue of(ClickHouseValue ref, byte[] bytes) {
        return ref instanceof ClickHouseStringValue ? ((ClickHouseStringValue) ref).set(bytes)
                : new ClickHouseStringValue(bytes);
    }

    private boolean binary;
    private byte[] bytes;
    private String value;

    protected ClickHouseStringValue(String value) {
        update(value);
    }

    protected ClickHouseStringValue(byte[] bytes) {
        update(bytes);
    }

    protected ClickHouseStringValue set(String value) {
        this.binary = false;
        this.bytes = null;
        this.value = value;
        return this;
    }

    protected ClickHouseStringValue set(byte[] bytes) {
        this.binary = true;
        this.bytes = bytes;
        this.value = null;
        return this;
    }

    @Override
    public ClickHouseStringValue copy(boolean deep) {
        if (bytes == null || !binary) {
            return new ClickHouseStringValue(value);
        }

        byte[] b = bytes;
        if (deep) {
            b = new byte[bytes.length];
            System.arraycopy(bytes, 0, b, 0, bytes.length);
        }
        return new ClickHouseStringValue(b);
    }

    public boolean isBinary() {
        return binary;
    }

    @Override
    public boolean isNullOrEmpty() {
        return bytes == null && value == null;
    }

    @Override
    public boolean asBoolean() {
        return ClickHouseValues.convertToBoolean(asString());
    }

    @Override
    public char asCharacter() {
        String s = asString();
        if (s == null) {
            return '\0';
        } else if (s.length() != 1) {
            throw new IllegalArgumentException(
                    ClickHouseUtils.format("Expect single-character string but we got [%s]", s));
        }
        return s.charAt(0);
    }

    @Override
    public byte asByte() {
        return isNullOrEmpty() ? (byte) 0 : Byte.parseByte(asString());
    }

    @Override
    public short asShort() {
        return isNullOrEmpty() ? (short) 0 : Short.parseShort(asString());
    }

    @Override
    public int asInteger() {
        return isNullOrEmpty() ? 0 : Integer.parseInt(asString());
    }

    @Override
    public long asLong() {
        return isNullOrEmpty() ? 0L : Long.parseLong(asString());
    }

    @Override
    public BigInteger asBigInteger() {
        return isNullOrEmpty() ? null : new BigInteger(asString());
    }

    @Override
    public float asFloat() {
        return isNullOrEmpty() ? 0F : Float.parseFloat(asString());
    }

    @Override
    public double asDouble() {
        return isNullOrEmpty() ? 0D : Double.parseDouble(asString());
    }

    @Override
    public BigDecimal asBigDecimal(int scale) {
        return isNullOrEmpty() ? null : new BigDecimal(asBigInteger(), scale);
    }

    @Override
    public LocalDate asDate() {
        return isNullOrEmpty() ? null : LocalDate.parse(asString(), ClickHouseValues.DATE_FORMATTER);
    }

    @Override
    public LocalTime asTime() {
        return isNullOrEmpty() ? null : LocalTime.parse(asString(), ClickHouseValues.TIME_FORMATTER);
    }

    @Override
    public LocalDateTime asDateTime(int scale) {
        return isNullOrEmpty() ? null : LocalDateTime.parse(asString(), ClickHouseValues.DATETIME_FORMATTER);
    }

    @Override
    public <T extends Enum<T>> T asEnum(Class<T> enumType) {
        return isNullOrEmpty() ? null : Enum.valueOf(enumType, asString());
    }

    @Override
    public Inet4Address asInet4Address() {
        return ClickHouseValues.convertToIpv4(asString());
    }

    @Override
    public Inet6Address asInet6Address() {
        return ClickHouseValues.convertToIpv6(asString());
    }

    @Override
    public Object asObject() {
        return asString();
    }

    @Override
    public Object asRawObject() {
        return isBinary() ? bytes : value;
    }

    @Override
    public byte[] asBinary() {
        if (value != null && bytes == null) {
            bytes = value.getBytes(StandardCharsets.UTF_8);
        }

        return bytes;
    }

    @Override
    public byte[] asBinary(int length, Charset charset) {
        byte[] b = bytes;
        if (value != null && b == null) {
            bytes = b = value.getBytes(charset == null ? StandardCharsets.UTF_8 : charset);
        }

        if (b != null && b.length < length) {
            b = Arrays.copyOf(b, length);
        }
        return b;
    }

    @Override
    public String asString() {
        if (bytes != null && value == null) {
            value = new String(bytes, StandardCharsets.UTF_8);
        }

        return value;
    }

    @Override
    public UUID asUuid() {
        return isNullOrEmpty() ? null : UUID.fromString(asString());
    }

    @Override
    public ClickHouseStringValue resetToDefault() {
        return set("");
    }

    @Override
    public ClickHouseStringValue resetToNullOrEmpty() {
        return set((String) null);
    }

    @Override
    public String toSqlExpression() {
        if (isNullOrEmpty()) {
            return ClickHouseValues.NULL_EXPR;
        } else if (binary) {
            return ClickHouseValues.convertToUnhexExpression(bytes);
        }
        return ClickHouseValues.convertToQuotedString(asString());
    }

    @Override
    public ClickHouseStringValue update(boolean value) {
        return set(String.valueOf(value));
    }

    @Override
    public ClickHouseStringValue update(char value) {
        // consistent with asCharacter()
        return set(String.valueOf((int) value));
    }

    @Override
    public ClickHouseStringValue update(byte value) {
        return set(String.valueOf(value));
    }

    @Override
    public ClickHouseStringValue update(byte[] value) {
        return set(value);
    }

    @Override
    public ClickHouseStringValue update(short value) {
        return set(String.valueOf(value));
    }

    @Override
    public ClickHouseStringValue update(int value) {
        return set(String.valueOf(value));
    }

    @Override
    public ClickHouseStringValue update(long value) {
        return set(String.valueOf(value));
    }

    @Override
    public ClickHouseStringValue update(float value) {
        return set(String.valueOf(value));
    }

    @Override
    public ClickHouseStringValue update(double value) {
        return set(String.valueOf(value));
    }

    @Override
    public ClickHouseValue update(BigInteger value) {
        return set(value == null ? null : String.valueOf(value));
    }

    @Override
    public ClickHouseValue update(BigDecimal value) {
        return set(value == null ? null : String.valueOf(value));
    }

    @Override
    public ClickHouseStringValue update(Enum<?> value) {
        return set(value == null ? null : value.name());
    }

    @Override
    public ClickHouseStringValue update(Inet4Address value) {
        return set(value == null ? null : value.getHostAddress());
    }

    @Override
    public ClickHouseStringValue update(Inet6Address value) {
        return set(value == null ? null : value.getHostAddress());
    }

    @Override
    public ClickHouseStringValue update(LocalDate value) {
        return set(value == null ? null : value.format(ClickHouseValues.DATE_FORMATTER));
    }

    @Override
    public ClickHouseStringValue update(LocalTime value) {
        return set(value == null ? null : value.format(ClickHouseValues.TIME_FORMATTER));
    }

    @Override
    public ClickHouseStringValue update(LocalDateTime value) {
        return set(value == null ? null : value.format(ClickHouseValues.DATETIME_FORMATTER));
    }

    @Override
    public ClickHouseStringValue update(String value) {
        return set(value);
    }

    @Override
    public ClickHouseStringValue update(UUID value) {
        return set(value == null ? null : value.toString());
    }

    @Override
    public ClickHouseStringValue update(ClickHouseValue value) {
        return set(value == null || value.isNullOrEmpty() ? null : value.asString());
    }

    @Override
    public ClickHouseStringValue update(Object value) {
        if (value instanceof byte[]) {
            return set((byte[]) value);
        } else if (value instanceof String) { // or CharSequence?
            return set((String) value);
        } else if (value instanceof ClickHouseStringValue) {
            ClickHouseStringValue s = (ClickHouseStringValue) value;
            return s.isBinary() ? set(s.asBinary()) : set(s.asString());
        }

        return set(value == null ? null : value.toString());
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (binary ? 1231 : 1237);
        result = prime * result + Arrays.hashCode(bytes);
        result = prime * result + ((value == null) ? 0 : value.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { // too bad this is a mutable class :<
            return true;
        } else if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ClickHouseStringValue v = (ClickHouseStringValue) obj;
        return binary == v.binary && Objects.equals(bytes, v.bytes) && Objects.equals(value, v.value);
    }

    @Override
    public String toString() {
        return ClickHouseValues.convertToString(this);
    }
}

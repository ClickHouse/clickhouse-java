package com.clickhouse.client.data;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Objects;
import java.util.UUID;
import com.clickhouse.client.ClickHouseChecker;
import com.clickhouse.client.ClickHouseValue;
import com.clickhouse.client.ClickHouseValues;

/**
 * Wraper class of string.
 */
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
        return ref instanceof ClickHouseStringValue ? ((ClickHouseStringValue) ref).set(null)
                : new ClickHouseStringValue(null);
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

    private String value;

    protected ClickHouseStringValue(String value) {
        update(value);
    }

    protected ClickHouseStringValue set(String value) {
        this.value = value;
        return this;
    }

    /**
     * Gets value.
     *
     * @return value
     */
    public String getValue() {
        return value;
    }

    @Override
    public ClickHouseStringValue copy(boolean deep) {
        return new ClickHouseStringValue(value);
    }

    @Override
    public boolean isNullOrEmpty() {
        return value == null;
    }

    @Override
    public boolean asBoolean() {
        // what about Y/N, Yes/No, enabled/disabled?
        return !isNullOrEmpty() && Boolean.parseBoolean(value);
    }

    @Override
    public byte asByte() {
        return isNullOrEmpty() ? (byte) 0 : Byte.parseByte(value);
    }

    @Override
    public short asShort() {
        return isNullOrEmpty() ? (short) 0 : Short.parseShort(value);
    }

    @Override
    public int asInteger() {
        return isNullOrEmpty() ? 0 : Integer.parseInt(value);
    }

    @Override
    public long asLong() {
        return isNullOrEmpty() ? 0L : Long.parseLong(value);
    }

    @Override
    public BigInteger asBigInteger() {
        return isNullOrEmpty() ? null : new BigInteger(value);
    }

    @Override
    public float asFloat() {
        return isNullOrEmpty() ? 0F : Float.parseFloat(value);
    }

    @Override
    public double asDouble() {
        return isNullOrEmpty() ? 0D : Double.parseDouble(value);
    }

    @Override
    public BigDecimal asBigDecimal(int scale) {
        return isNullOrEmpty() ? null : new BigDecimal(asBigInteger(), scale);
    }

    @Override
    public LocalDate asDate() {
        return isNullOrEmpty() ? null : LocalDate.parse(value, ClickHouseValues.DATE_FORMATTER);
    }

    @Override
    public LocalTime asTime() {
        return isNullOrEmpty() ? null : LocalTime.parse(value, ClickHouseValues.TIME_FORMATTER);
    }

    @Override
    public LocalDateTime asDateTime(int scale) {
        return isNullOrEmpty() ? null : LocalDateTime.parse(value, ClickHouseValues.DATETIME_FORMATTER);
    }

    @Override
    public <T extends Enum<T>> T asEnum(Class<T> enumType) {
        return isNullOrEmpty() ? null : Enum.valueOf(enumType, value);
    }

    @Override
    public Inet4Address asInet4Address() {
        return ClickHouseValues.convertToIpv4(getValue());
    }

    @Override
    public Inet6Address asInet6Address() {
        return ClickHouseValues.convertToIpv6(getValue());
    }

    @Override
    public Object asObject() {
        return value;
    }

    @Override
    public String asString(int length, Charset charset) {
        if (value != null && length > 0) {
            ClickHouseChecker.notWithDifferentLength(value.getBytes(charset == null ? StandardCharsets.UTF_8 : charset),
                    length);
        }

        return value;
    }

    @Override
    public UUID asUuid() {
        return isNullOrEmpty() ? null : UUID.fromString(value);
    }

    @Override
    public ClickHouseStringValue resetToNullOrEmpty() {
        return set(null);
    }

    @Override
    public String toSqlExpression() {
        return ClickHouseValues.convertToQuotedString(value);
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
        return set(value == null ? null : value.asString());
    }

    @Override
    public ClickHouseStringValue update(Object value) {
        return update(value == null ? null : value.toString());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { // too bad this is a mutable class :<
            return true;
        } else if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ClickHouseStringValue v = (ClickHouseStringValue) obj;
        return value == v.value || (value != null && value.equals(v.value));
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public String toString() {
        return ClickHouseValues.convertToString(this);
    }
}

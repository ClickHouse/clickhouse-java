package com.clickhouse.client;

import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.Map.Entry;

import com.clickhouse.client.data.ClickHouseArrayValue;
import com.clickhouse.client.data.ClickHouseBigDecimalValue;
import com.clickhouse.client.data.ClickHouseBigIntegerValue;
import com.clickhouse.client.data.ClickHouseBitmapValue;
import com.clickhouse.client.data.ClickHouseByteValue;
import com.clickhouse.client.data.ClickHouseDateTimeValue;
import com.clickhouse.client.data.ClickHouseDateValue;
import com.clickhouse.client.data.ClickHouseDoubleValue;
import com.clickhouse.client.data.ClickHouseEmptyValue;
import com.clickhouse.client.data.ClickHouseFloatValue;
import com.clickhouse.client.data.ClickHouseGeoMultiPolygonValue;
import com.clickhouse.client.data.ClickHouseGeoPointValue;
import com.clickhouse.client.data.ClickHouseGeoPolygonValue;
import com.clickhouse.client.data.ClickHouseGeoRingValue;
import com.clickhouse.client.data.ClickHouseIntegerValue;
import com.clickhouse.client.data.ClickHouseIpv4Value;
import com.clickhouse.client.data.ClickHouseIpv6Value;
import com.clickhouse.client.data.ClickHouseLongValue;
import com.clickhouse.client.data.ClickHouseMapValue;
import com.clickhouse.client.data.ClickHouseNestedValue;
import com.clickhouse.client.data.ClickHouseOffsetDateTimeValue;
import com.clickhouse.client.data.ClickHouseShortValue;
import com.clickhouse.client.data.ClickHouseStringValue;
import com.clickhouse.client.data.ClickHouseTupleValue;
import com.clickhouse.client.data.ClickHouseUuidValue;
import com.clickhouse.client.data.array.ClickHouseByteArrayValue;
import com.clickhouse.client.data.array.ClickHouseDoubleArrayValue;
import com.clickhouse.client.data.array.ClickHouseFloatArrayValue;
import com.clickhouse.client.data.array.ClickHouseIntArrayValue;
import com.clickhouse.client.data.array.ClickHouseLongArrayValue;
import com.clickhouse.client.data.array.ClickHouseShortArrayValue;

/**
 * Help class for dealing with values.
 */
public final class ClickHouseValues {
    public static final BigInteger BIGINT_HL_BOUNDARY = BigInteger.ONE.shiftLeft(64); // 2^64
    public static final BigInteger BIGINT_SL_BOUNDARY = BigInteger.valueOf(Long.MAX_VALUE);

    public static final LocalDate DATE_ZERO = LocalDate.ofEpochDay(0L);
    public static final LocalDateTime DATETIME_ZERO = LocalDateTime.ofEpochSecond(0L, 0, ZoneOffset.UTC);
    public static final LocalTime TIME_ZERO = LocalTime.ofSecondOfDay(0L);

    public static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];
    public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    public static final short[] EMPTY_SHORT_ARRAY = new short[0];
    public static final int[] EMPTY_INT_ARRAY = new int[0];
    public static final long[] EMPTY_LONG_ARRAY = new long[0];
    public static final float[] EMPTY_FLOAT_ARRAY = new float[0];
    public static final double[] EMPTY_DOUBLE_ARRAY = new double[0];
    public static final String EMPTY_ARRAY_EXPR = "[]";

    public static final BigDecimal NANOS = new BigDecimal(BigInteger.TEN.pow(9));

    public static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    public static final DateTimeFormatter TIME_FORMATTER = new DateTimeFormatterBuilder().appendPattern("HH:mm:ss")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).toFormatter();
    public static final DateTimeFormatter DATETIME_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss").appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).toFormatter();

    public static final TimeZone UTC_TIMEZONE = TimeZone.getTimeZone("UTC");
    public static final ZoneId UTC_ZONE = UTC_TIMEZONE.toZoneId();

    public static final String NULL_EXPR = "NULL";
    public static final String NAN_EXPR = "NaN";
    public static final String INF_EXPR = "Inf";
    public static final String NINF_EXPR = "-Inf";

    public static final String ERROR_INVALID_POINT = "A point should have two and only two double values, but we got: ";
    public static final String ERROR_SINGLETON_ARRAY = "Only singleton array is allowed, but we got: ";
    public static final String ERROR_SINGLETON_COLLECTION = "Only singleton collection is allowed, but we got: ";
    public static final String ERROR_SINGLETON_MAP = "Only singleton map is allowed, but we got: ";

    public static final String PARAM_PRECISION = "precision";
    public static final String PARAM_SCALE = "scale";

    public static final String TYPE_BOOLEAN = "boolean";
    public static final String TYPE_CHAR = "char";
    public static final String TYPE_BYTE = "byte";
    public static final String TYPE_SHORT = "short";
    public static final String TYPE_INT = "int";
    public static final String TYPE_LONG = "long";
    public static final String TYPE_FLOAT = "float";
    public static final String TYPE_DOUBLE = "double";
    public static final String TYPE_BIG_DECIMAL = "BigDecimal";
    public static final String TYPE_BIG_INTEGER = "BigInteger";
    public static final String TYPE_DATE = "Date";
    public static final String TYPE_TIME = "Time";
    public static final String TYPE_DATE_TIME = "DateTime";
    public static final String TYPE_ENUM = "Enum";
    public static final String TYPE_IPV4 = "Inet4Address";
    public static final String TYPE_IPV6 = "Inet6Address";
    public static final String TYPE_STRING = "String";
    public static final String TYPE_UUID = "UUID";
    public static final String TYPE_OBJECT = "Object";
    public static final String TYPE_ARRAY = "Array";
    public static final String TYPE_MAP = "Map";
    public static final String TYPE_NESTED = "Nested";
    public static final String TYPE_TUPLE = "Tuple";
    public static final String TYPE_POINT = "Point";
    public static final String TYPE_RING = "Ring";
    public static final String TYPE_POLYGON = "Polygon";
    public static final String TYPE_MULTI_POLYGON = "MultiPolygon";

    public static final String TYPE_CLASS = "Class";

    /**
     * Converts IP address to big integer.
     *
     * @param value IP address
     * @return big integer
     */
    public static BigInteger convertToBigInteger(Inet4Address value) {
        return value == null ? null : new BigInteger(1, value.getAddress());
    }

    /**
     * Converts IP address to big integer.
     *
     * @param value IP address
     * @return big integer
     */

    public static BigInteger convertToBigInteger(Inet6Address value) {
        return value == null ? null : new BigInteger(1, value.getAddress());
    }

    /**
     * Converts UUID to big integer.
     *
     * @param value UUID
     * @return big integer
     */

    public static BigInteger convertToBigInteger(UUID value) {
        if (value == null) {
            return null;
        }

        BigInteger high = BigInteger.valueOf(value.getMostSignificantBits());
        BigInteger low = BigInteger.valueOf(value.getLeastSignificantBits());

        if (high.signum() < 0) {
            high = high.add(BIGINT_HL_BOUNDARY);
        }
        if (low.signum() < 0) {
            low = low.add(BIGINT_HL_BOUNDARY);
        }

        return low.add(high.multiply(BIGINT_HL_BOUNDARY));
    }

    /**
     * Converts big decimal to date time.
     *
     * @param value big decimal
     * @return date time
     */
    public static LocalDateTime convertToDateTime(BigDecimal value) {
        if (value == null) {
            return null;
        } else if (value.scale() == 0) {
            return LocalDateTime.ofEpochSecond(value.longValue(), 0, ZoneOffset.UTC);
        } else if (value.signum() >= 0) {
            return LocalDateTime.ofEpochSecond(value.longValue(),
                    value.remainder(BigDecimal.ONE).multiply(NANOS).intValue(), ZoneOffset.UTC);
        }

        long v = NANOS.add(value.remainder(BigDecimal.ONE).multiply(NANOS)).longValue();
        int nanoSeconds = v < 1000000000L ? (int) v : 0;

        return LocalDateTime.ofEpochSecond(value.longValue() - (nanoSeconds > 0 ? 1 : 0), nanoSeconds, ZoneOffset.UTC);
    }

    /**
     * Converts big decimal to date time.
     *
     * @param value big decimal
     * @param tz    time zone, null is treated as UTC
     * @return date time
     */
    public static ZonedDateTime convertToDateTime(BigDecimal value, TimeZone tz) {
        if (value == null) {
            return null;
        }

        return convertToDateTime(value, tz != null ? tz.toZoneId() : UTC_ZONE);
    }

    /**
     * Converts big decimal to date time.
     *
     * @param value big decimal
     * @param zone  zone id, null is treated as UTC
     * @return date time
     */
    public static ZonedDateTime convertToDateTime(BigDecimal value, ZoneId zone) {
        if (value == null) {
            return null;
        }

        if (zone == null) {
            zone = UTC_ZONE;
        }
        if (value.scale() == 0) {
            return Instant.ofEpochSecond(value.longValue(), 0L).atZone(zone);
        } else if (value.signum() >= 0) {
            return Instant.ofEpochSecond(value.longValue(), value.remainder(BigDecimal.ONE).multiply(NANOS).intValue())
                    .atZone(zone);
        }

        long v = NANOS.add(value.remainder(BigDecimal.ONE).multiply(NANOS)).longValue();
        int nanoSeconds = v < 1000000000L ? (int) v : 0;

        return Instant.ofEpochSecond(value.longValue() - (nanoSeconds > 0 ? 1 : 0), nanoSeconds).atZone(zone);
    }

    /**
     * Converts big decimal to date time.
     *
     * @param value  big decimal
     * @param offset zone offset, null is treated as {@code ZoneOffset.UTC}
     * @return date time
     */
    public static OffsetDateTime convertToDateTime(BigDecimal value, ZoneOffset offset) {
        if (value == null) {
            return null;
        }

        if (offset == null) {
            offset = ZoneOffset.UTC;
        }
        if (value.scale() == 0) {
            return Instant.ofEpochSecond(value.longValue(), 0L).atOffset(offset);
        } else if (value.signum() >= 0) {
            return Instant.ofEpochSecond(value.longValue(), value.remainder(BigDecimal.ONE).multiply(NANOS).intValue())
                    .atOffset(offset);
        }

        long v = NANOS.add(value.remainder(BigDecimal.ONE).multiply(NANOS)).longValue();
        int nanoSeconds = v < 1000000000L ? (int) v : 0;

        return Instant.ofEpochSecond(value.longValue() - (nanoSeconds > 0 ? 1 : 0), nanoSeconds).atOffset(offset);
    }

    /**
     * Converts IPv6 address to IPv4 address if applicable.
     *
     * @param value IPv6 address
     * @return IPv4 address
     * @throws IllegalArgumentException when failed to convert to IPv4 address
     */
    public static Inet4Address convertToIpv4(Inet6Address value) {
        if (value == null) {
            return null;
        }

        byte[] bytes = value.getAddress();
        boolean invalid = false;
        for (int i = 0; i < 10; i++) {
            if (bytes[i] != (byte) 0) {
                invalid = true;
                break;
            }
        }

        if (!invalid) {
            invalid = bytes[10] != 0xFF || bytes[11] != 0xFF;
        }

        if (invalid) {
            throw new IllegalArgumentException("Failed to convert IPv6 to IPv4");
        }

        byte[] addr = new byte[4];
        System.arraycopy(bytes, 12, addr, 0, 4);
        try {
            return (Inet4Address) InetAddress.getByAddress(addr);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Converts integer to IPv4 address.
     *
     * @param value integer
     * @return IPv4 address
     */
    public static Inet4Address convertToIpv4(int value) {
        byte[] bytes = new byte[] { (byte) ((value >> 24) & 0xFF), (byte) ((value >> 16) & 0xFF),
                (byte) ((value >> 8) & 0xFF), (byte) (value & 0xFF) };

        try {
            return (Inet4Address) InetAddress.getByAddress(bytes);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Converts string to IPv4 address.
     *
     * @param value string
     * @return IPv4 address
     * @throws IllegalArgumentException when failed to convert to IPv4 address
     */
    public static Inet4Address convertToIpv4(String value) {
        if (value == null) {
            return null;
        }

        try {
            for (InetAddress addr : InetAddress.getAllByName(value)) {
                if (addr instanceof Inet4Address) {
                    return (Inet4Address) addr;
                }
            }
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException(ClickHouseUtils.format("Failed to convert [%s] to Inet4Address", value),
                    e);
        }

        throw new IllegalArgumentException(ClickHouseUtils.format("No Inet4Address for [%s]", value));
    }

    /**
     * Converts big integer to IPv6 address.
     *
     * @param value big integer
     * @return IPv6 address
     * @throws IllegalArgumentException when failed to convert to IPv6 address
     */
    public static Inet6Address convertToIpv6(BigInteger value) {
        if (value == null) {
            return null;
        }

        byte[] bytes = ClickHouseChecker.nonNull(value, "value").toByteArray();
        int len = bytes.length;
        if (len > 16) {
            throw new IllegalArgumentException("The number is too large to be converted to IPv6: " + value);
        } else if (len < 16) {
            byte[] addr = new byte[16];
            int diff = 16 - len;
            for (int i = len - 1; i >= 0; i--) {
                addr[i + diff] = bytes[i];
            }
            bytes = addr;
        }

        try {
            return Inet6Address.getByAddress(null, bytes, null);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Converts IPv4 address to IPv6 address.
     *
     * @param value IPv4 address
     * @return IPv6 address
     * @throws IllegalArgumentException when failed to convert to IPv6 address
     */
    public static Inet6Address convertToIpv6(Inet4Address value) {
        if (value == null) {
            return null;
        }

        // https://en.wikipedia.org/wiki/IPv6#IPv4-mapped_IPv6_addresses
        byte[] bytes = new byte[16];
        bytes[10] = (byte) 0xFF;
        bytes[11] = (byte) 0xFF;
        System.arraycopy(value.getAddress(), 0, bytes, 12, 4);

        try {
            return Inet6Address.getByAddress(null, bytes, null);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Converts string to IPv6 address.
     *
     * @param value string
     * @return IPv6 address
     * @throws IllegalArgumentException when failed to convert to IPv6 address
     */
    public static Inet6Address convertToIpv6(String value) {
        if (value == null) {
            return null;
        }

        try {
            for (InetAddress addr : InetAddress.getAllByName(value)) {
                if (addr instanceof Inet6Address) {
                    return (Inet6Address) addr;
                }
            }
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException(ClickHouseUtils.format("Failed to convert [%s] to Inet6Address", value),
                    e);
        }

        throw new IllegalArgumentException(ClickHouseUtils.format("No Inet6Address for [%s]", value));
    }

    /**
     * Converts abitrary object to an expression that can be used in SQL query.
     *
     * @param value value may or may not be null
     * @return escaped SQL expression
     */
    @SuppressWarnings({ "unchecked", "squid:S3776" })
    public static String convertToSqlExpression(Object value) {
        if (value == null) {
            return NULL_EXPR;
        }

        String s;
        if (value instanceof String) {
            s = convertToQuotedString(value);
        } else if (value instanceof ClickHouseValue) {
            s = ((ClickHouseValue) value).toSqlExpression();
        } else if (value instanceof UUID) {
            s = new StringBuilder().append('\'').append(value).append('\'').toString();
        } else if (value instanceof LocalDate) {
            s = new StringBuilder().append('\'').append(((LocalDate) value).format(DATE_FORMATTER)).append('\'')
                    .toString();
        } else if (value instanceof LocalTime) { // currently not supported in ClickHouse
            s = new StringBuilder().append('\'').append(((LocalTime) value).format(TIME_FORMATTER)).append('\'')
                    .toString();
        } else if (value instanceof LocalDateTime) {
            s = new StringBuilder().append('\'').append(((LocalDateTime) value).format(DATETIME_FORMATTER)).append('\'')
                    .toString();
        } else if (value instanceof OffsetDateTime) {
            s = new StringBuilder().append('\'').append(((OffsetDateTime) value).format(DATETIME_FORMATTER))
                    .append('\'').toString();
        } else if (value instanceof ZonedDateTime) {
            s = new StringBuilder().append('\'').append(((ZonedDateTime) value).format(DATETIME_FORMATTER)).append('\'')
                    .toString();
        } else if (value instanceof InetAddress) {
            s = new StringBuilder().append('\'').append(((InetAddress) value).getHostAddress()).append('\'').toString();
        } else if (value instanceof Enum) {
            s = String.valueOf(((Enum<?>) value).ordinal()); // faster than escaped name
        } else if (value instanceof Object[]) { // array & nested
            StringBuilder builder = new StringBuilder().append('[');
            for (Object o : (Object[]) value) {
                builder.append(convertToSqlExpression(o)).append(',');
            }
            if (builder.length() > 1) {
                builder.setLength(builder.length() - 1);
            }
            s = builder.append(']').toString();
        } else if (value instanceof Collection) { // treat as tuple
            StringBuilder builder = new StringBuilder().append('(');
            for (Object v : (Collection<Object>) value) {
                builder.append(convertToSqlExpression(v)).append(',');
            }
            if (builder.length() > 1) {
                builder.setLength(builder.length() - 1);
            }
            s = builder.append(')').toString();
        } else if (value instanceof Enumeration) { // treat as tuple
            StringBuilder builder = new StringBuilder().append('(');
            Enumeration<Object> v = (Enumeration<Object>) value;
            while (v.hasMoreElements()) {
                builder.append(convertToSqlExpression(v.nextElement())).append(',');
            }
            if (builder.length() > 1) {
                builder.setLength(builder.length() - 1);
            }
            s = builder.append(')').toString();
        } else if (value instanceof Map) { // map
            StringBuilder builder = new StringBuilder().append('{');
            for (Entry<Object, Object> v : ((Map<Object, Object>) value).entrySet()) {
                builder.append(convertToSqlExpression(v.getKey())).append(" : ")
                        .append(convertToSqlExpression(v.getValue())).append(',');
            }
            if (builder.length() > 1) {
                builder.setLength(builder.length() - 1);
            }
            s = builder.append('}').toString();
        } else if (value instanceof Boolean) {
            s = String.valueOf((boolean) value ? 1 : 0);
        } else if (value instanceof Character) {
            s = String.valueOf((int) ((char) value));
        } else if (value instanceof boolean[]) {
            s = convertToString((boolean[]) value);
        } else if (value instanceof char[]) {
            s = convertToString((char[]) value);
        } else if (value instanceof byte[]) {
            s = convertToString((byte[]) value);
        } else if (value instanceof short[]) {
            s = convertToString((short[]) value);
        } else if (value instanceof int[]) {
            s = convertToString((int[]) value);
        } else if (value instanceof long[]) {
            s = convertToString((long[]) value);
        } else if (value instanceof float[]) {
            s = convertToString((float[]) value);
        } else if (value instanceof double[]) {
            s = convertToString((double[]) value);
        } else {
            s = String.valueOf(value);
        }
        return s;
    }

    /**
     * Converts boolean array to compact string. Similar as
     * {@code Arrays.toString()} but without any whitespace.
     * 
     * @param value boolean array
     * @return string
     */
    public static String convertToString(boolean[] value) {
        if (value == null) {
            return NULL_EXPR;
        } else if (value.length == 0) {
            return EMPTY_ARRAY_EXPR;
        }

        StringBuilder builder = new StringBuilder().append('[');
        for (boolean v : value) {
            builder.append(v ? 1 : 0).append(',');
        }
        builder.setLength(builder.length() - 1);
        return builder.append(']').toString();
    }

    /**
     * Converts character array to compact string. Similar as
     * {@code Arrays.toString()} but without any whitespace.
     * 
     * @param value character array
     * @return string
     */
    public static String convertToString(char[] value) {
        if (value == null) {
            return NULL_EXPR;
        } else if (value.length == 0) {
            return EMPTY_ARRAY_EXPR;
        }

        StringBuilder builder = new StringBuilder().append('[');
        for (char v : value) {
            builder.append((int) v).append(',');
        }
        builder.setLength(builder.length() - 1);
        return builder.append(']').toString();
    }

    /**
     * Converts byte array to compact string. Similar as {@code Arrays.toString()}
     * but without any whitespace.
     * 
     * @param value byte array
     * @return string
     */
    public static String convertToString(byte[] value) {
        if (value == null) {
            return NULL_EXPR;
        } else if (value.length == 0) {
            return EMPTY_ARRAY_EXPR;
        }

        StringBuilder builder = new StringBuilder().append('[');
        for (byte v : value) {
            builder.append(v).append(',');
        }
        builder.setLength(builder.length() - 1);
        return builder.append(']').toString();
    }

    /**
     * Converts short array to compact string. Similar as {@code Arrays.toString()}
     * but without any whitespace.
     * 
     * @param value short array
     * @return string
     */
    public static String convertToString(short[] value) {
        if (value == null) {
            return NULL_EXPR;
        } else if (value.length == 0) {
            return EMPTY_ARRAY_EXPR;
        }

        StringBuilder builder = new StringBuilder().append('[');
        for (short v : value) {
            builder.append(v).append(',');
        }
        builder.setLength(builder.length() - 1);
        return builder.append(']').toString();
    }

    /**
     * Converts integer array to compact string. Similar as
     * {@code Arrays.toString()} but without any whitespace.
     * 
     * @param value integer array
     * @return string
     */
    public static String convertToString(int[] value) {
        if (value == null) {
            return NULL_EXPR;
        } else if (value.length == 0) {
            return EMPTY_ARRAY_EXPR;
        }

        StringBuilder builder = new StringBuilder().append('[');
        for (int v : value) {
            builder.append(v).append(',');
        }
        builder.setLength(builder.length() - 1);
        return builder.append(']').toString();
    }

    /**
     * Converts long array to compact string. Similar as {@code Arrays.toString()}
     * but without any whitespace.
     * 
     * @param value long array
     * @return string
     */
    public static String convertToString(long[] value) {
        if (value == null) {
            return NULL_EXPR;
        } else if (value.length == 0) {
            return EMPTY_ARRAY_EXPR;
        }

        StringBuilder builder = new StringBuilder().append('[');
        for (long v : value) {
            builder.append(v).append(',');
        }
        builder.setLength(builder.length() - 1);
        return builder.append(']').toString();
    }

    /**
     * Converts float array to compact string. Similar as {@code Arrays.toString()}
     * but without any whitespace.
     * 
     * @param value float array
     * @return string
     */
    public static String convertToString(float[] value) {
        if (value == null) {
            return NULL_EXPR;
        } else if (value.length == 0) {
            return EMPTY_ARRAY_EXPR;
        }

        StringBuilder builder = new StringBuilder().append('[');
        for (float v : value) {
            builder.append(v).append(',');
        }
        builder.setLength(builder.length() - 1);
        return builder.append(']').toString();
    }

    /**
     * Converts double array to compact string. Similar as {@code Arrays.toString()}
     * but without any whitespace.
     * 
     * @param value double array
     * @return string
     */
    public static String convertToString(double[] value) {
        if (value == null) {
            return NULL_EXPR;
        } else if (value.length == 0) {
            return EMPTY_ARRAY_EXPR;
        }

        StringBuilder builder = new StringBuilder().append('[');
        for (double v : value) {
            builder.append(v).append(',');
        }
        builder.setLength(builder.length() - 1);
        return builder.append(']').toString();
    }

    /**
     * Converts given value object to string.
     *
     * @param value value object
     * @return string
     */
    public static String convertToString(ClickHouseValue value) {
        return value == null ? "null"
                : new StringBuilder().append(value.getClass().getSimpleName()).append('[').append(value.asString())
                        .append(']').toString();
    }

    /**
     * Converts object to string. Same as {@code String.valueOf()}.
     *
     * @param value object may or may not be null
     * @return string representation of the object
     */
    public static String convertToString(Object value) {
        return String.valueOf(value);
    }

    /**
     * Converts big integer to UUID.
     *
     * @param value big integer
     * @return UUID
     */
    public static UUID convertToUuid(BigInteger value) {
        if (value == null) {
            return null;
        }

        BigInteger[] parts = value.divideAndRemainder(BIGINT_HL_BOUNDARY);
        BigInteger high = parts[0];
        BigInteger low = parts[1];

        if (BIGINT_SL_BOUNDARY.compareTo(high) < 0) {
            high = high.subtract(BIGINT_HL_BOUNDARY);
        }
        if (BIGINT_SL_BOUNDARY.compareTo(low) < 0) {
            low = low.subtract(BIGINT_HL_BOUNDARY);
        }

        return new UUID(high.longValueExact(), low.longValueExact());
    }

    /**
     * Converts object to quoted string.
     *
     * @param value object may or may not be null
     * @return quoted string representing the object
     */
    public static String convertToQuotedString(Object value) {
        if (value == null) {
            return NULL_EXPR;
        }

        return new StringBuilder().append('\'').append(ClickHouseUtils.escape(value.toString(), '\'')).append('\'')
                .toString();
    }

    /**
     * Creates an object array. Primitive types will be converted to corresponding
     * wrapper types, also {@code Boolean} / {@code boolean} will be converted to
     * {@code Byte}, and {@code Character} / {@code char} to {@code Integer}.
     *
     * @param <T>    type of the base element
     * @param clazz  class of the base element, null is treated as
     *               {@code Object.class}
     * @param length length of the array, negative is treated as zero
     * @param level  level of the array, must between 1 and 255
     * @return a non-null object array
     */
    @SuppressWarnings("unchecked")
    public static <T extends Object> T[] createObjectArray(Class<T> clazz, int length, int level) {
        if (level < 1) {
            level = 1;
        } else if (level > 255) {
            level = 255;
        }
        int[] dimensions = new int[level];
        dimensions[0] = length < 0 ? 0 : length;
        return (T[]) Array.newInstance(ClickHouseDataType.toObjectType(clazz), dimensions);
    }

    /**
     * Creates a primitive array if applicable. Wrapper types will be converted to
     * corresponding primitive types, also {@code Boolean} / {@code boolean} will be
     * converted to {@code byte}, and {@code Character} / {@code char} to
     * {@code int}.
     * 
     * @param clazz  class of the base element
     * @param length length of the array, negative is treated as zero
     * @param level  level of the array, must between 1 and 255
     * @return a primitive array if applicable; an object array otherwise
     */
    public static Object createPrimitiveArray(Class<?> clazz, int length, int level) {
        if (level < 1) {
            level = 1;
        } else if (level > 255) {
            level = 255;
        }
        int[] dimensions = new int[level];
        dimensions[0] = length < 0 ? 0 : length;
        return Array.newInstance(ClickHouseDataType.toPrimitiveType(clazz), dimensions);
    }

    static ClickHouseArrayValue<?> fillByteArray(ClickHouseArrayValue<?> array, ClickHouseConfig config,
            ClickHouseColumn column, ClickHouseInputStream input, ClickHouseDeserializer<ClickHouseValue> deserializer,
            int length) throws IOException {
        byte[] values = new byte[length];
        ClickHouseValue ref = ClickHouseByteValue.ofNull();
        for (int i = 0; i < length; i++) {
            values[i] = deserializer.deserialize(ref, config, column, input).asByte();
        }
        return array.update(values);
    }

    /**
     * Extract one and only value from singleton collection.
     *
     * @param value singleton collection
     * @return value
     * @throws IllegalArgumentException if the given collection is null or contains
     *                                  zero or more than one element
     */
    public static Object extractSingleValue(Collection<?> value) {
        if (value == null || value.size() != 1) {
            throw new IllegalArgumentException(ERROR_SINGLETON_COLLECTION + value);
        }

        return value.iterator().next();
    }

    /**
     * Extract one and only value from singleton enumeration.
     *
     * @param value singleton enumeration
     * @return value
     * @throws IllegalArgumentException if the given enumeration is null or contains
     *                                  zero or more than one element
     */
    public static Object extractSingleValue(Enumeration<?> value) {
        if (value == null || !value.hasMoreElements()) {
            throw new IllegalArgumentException(ERROR_SINGLETON_COLLECTION + value);
        }

        Object v = value.nextElement();
        if (value.hasMoreElements()) {
            throw new IllegalArgumentException(ERROR_SINGLETON_COLLECTION + value);
        }

        return v;
    }

    /**
     * Extract one and only value from singleton map - key will be ignored.
     *
     * @param value singleton map
     * @return value
     * @throws IllegalArgumentException if the given map is null or contains zero or
     *                                  more than one element
     */
    public static Object extractSingleValue(Map<?, ?> value) {
        if (value == null || value.size() != 1) {
            throw new IllegalArgumentException(ERROR_SINGLETON_MAP + value);
        }

        return value.values().iterator().next();
    }

    /**
     * Creates a value object based on given column.
     *
     * @param column non-null column
     * @return value object with default value, either null or empty
     */
    public static ClickHouseValue newValue(ClickHouseColumn column) {
        return newValue(ClickHouseChecker.nonNull(column, "column").getDataType(), column);
    }

    /**
     * Creates a value object based on given data type.
     *
     * @param type non-null data type
     * @return value object with default value, either null or empty
     */
    public static ClickHouseValue newValue(ClickHouseDataType type) {
        return newValue(ClickHouseChecker.nonNull(type, "type"), null);
    }

    private static ClickHouseValue newValue(ClickHouseDataType type, ClickHouseColumn column) {
        ClickHouseValue value = null;
        switch (type) { // still faster than EnumMap and with less overhead
            case Enum:
            case Enum8:
            case Int8:
                value = ClickHouseByteValue.ofNull();
                break;
            case UInt8:
            case Enum16:
            case Int16:
                value = ClickHouseShortValue.ofNull();
                break;
            case UInt16:
            case Int32:
                value = ClickHouseIntegerValue.ofNull();
                break;
            case UInt32:
            case IntervalYear:
            case IntervalQuarter:
            case IntervalMonth:
            case IntervalWeek:
            case IntervalDay:
            case IntervalHour:
            case IntervalMinute:
            case IntervalSecond:
            case Int64:
                value = ClickHouseLongValue.ofNull(false);
                break;
            case UInt64:
                value = ClickHouseLongValue.ofNull(true);
                break;
            case Int128:
            case UInt128:
            case Int256:
            case UInt256:
                value = ClickHouseBigIntegerValue.ofNull();
                break;
            case Float32:
                value = ClickHouseFloatValue.ofNull();
                break;
            case Float64:
                value = ClickHouseDoubleValue.ofNull();
                break;
            case Decimal:
            case Decimal32:
            case Decimal64:
            case Decimal128:
            case Decimal256:
                value = ClickHouseBigDecimalValue.ofNull();
                break;
            case Date:
            case Date32:
                value = ClickHouseDateValue.ofNull();
                break;
            case DateTime:
            case DateTime32:
            case DateTime64: {
                if (column == null) {
                    value = ClickHouseDateTimeValue.ofNull(0);
                } else if (column.getTimeZone() == null) {
                    value = ClickHouseDateTimeValue.ofNull(column.getScale());
                } else {
                    value = ClickHouseOffsetDateTimeValue.ofNull(column.getScale(), column.getTimeZone());
                }
                break;
            }
            case IPv4:
                value = ClickHouseIpv4Value.ofNull();
                break;
            case IPv6:
                value = ClickHouseIpv6Value.ofNull();
                break;
            case FixedString:
            case String:
                value = ClickHouseStringValue.ofNull();
                break;
            case UUID:
                value = ClickHouseUuidValue.ofNull();
                break;
            case Point:
                value = ClickHouseGeoPointValue.ofOrigin();
                break;
            case Ring:
                value = ClickHouseGeoRingValue.ofEmpty();
                break;
            case Polygon:
                value = ClickHouseGeoPolygonValue.ofEmpty();
                break;
            case MultiPolygon:
                value = ClickHouseGeoMultiPolygonValue.ofEmpty();
                break;
            case AggregateFunction:
                value = ClickHouseEmptyValue.INSTANCE;
                if (column != null) {
                    switch (column.getAggregateFunction()) {
                        case any:
                            value = newValue(column.getNestedColumns().get(0));
                            break;
                        case groupBitmap:
                            value = ClickHouseBitmapValue.ofEmpty(column.getNestedColumns().get(0).getDataType());
                            break;
                        default:
                            break;
                    }
                }
                break;
            case Array:
                if (column == null) {
                    value = ClickHouseArrayValue.ofEmpty();
                } else if (column.getArrayNestedLevel() > 1) {
                    value = ClickHouseArrayValue.of(
                            (Object[]) createPrimitiveArray(
                                    column.getArrayBaseColumn().getDataType().getPrimitiveClass(),
                                    0, column.getArrayNestedLevel()));
                } else {
                    Class<?> javaClass = column.getArrayBaseColumn().getDataType().getPrimitiveClass();
                    if (byte.class == javaClass) {
                        value = ClickHouseByteArrayValue.ofEmpty();
                    } else if (short.class == javaClass) {
                        value = ClickHouseShortArrayValue.ofEmpty();
                    } else if (int.class == javaClass) {
                        value = ClickHouseIntArrayValue.ofEmpty();
                    } else if (long.class == javaClass) {
                        value = ClickHouseLongArrayValue.ofEmpty();
                    } else if (float.class == javaClass) {
                        value = ClickHouseFloatArrayValue.ofEmpty();
                    } else if (double.class == javaClass) {
                        value = ClickHouseDoubleArrayValue.ofEmpty();
                    } else {
                        value = ClickHouseArrayValue.ofEmpty();
                    }
                }
                break;
            case Map:
                if (column == null) {
                    throw new IllegalArgumentException("column types for key and value are required");
                }
                value = ClickHouseMapValue.ofEmpty(column.getKeyInfo().getDataType().getObjectClass(),
                        column.getValueInfo().getDataType().getObjectClass());
                break;
            case Nested:
                if (column == null) {
                    throw new IllegalArgumentException("nested column types are required");
                }
                value = ClickHouseNestedValue.ofEmpty(column.getNestedColumns());
                break;
            case Tuple:
                value = ClickHouseTupleValue.of();
                break;
            case Nothing:
                value = ClickHouseEmptyValue.INSTANCE;
                break;
            default:
                break;
        }

        if (value == null) {
            throw new IllegalArgumentException("Unsupported data type: " + type.name());
        }

        return value;
    }

    private ClickHouseValues() {
    }
}

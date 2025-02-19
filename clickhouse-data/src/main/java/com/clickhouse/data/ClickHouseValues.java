package com.clickhouse.data;

import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
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
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.Map.Entry;

import com.clickhouse.data.value.ClickHouseArrayValue;
import com.clickhouse.data.value.ClickHouseByteValue;
import com.clickhouse.data.value.ClickHouseIpv4Value;
import com.clickhouse.data.value.ClickHouseIpv6Value;

/**
 * Help class for dealing with values.
 */
@Deprecated
public final class ClickHouseValues {
    public static final BigInteger BIGINT_HL_BOUNDARY = BigInteger.ONE.shiftLeft(64); // 2^64
    public static final BigInteger BIGINT_SL_BOUNDARY = BigInteger.valueOf(Long.MAX_VALUE);

    public static final LocalDate DATE_ZERO = LocalDate.ofEpochDay(0L);
    public static final LocalDateTime DATETIME_ZERO = LocalDateTime.ofEpochSecond(0L, 0, ZoneOffset.UTC);
    public static final LocalTime TIME_ZERO = LocalTime.ofSecondOfDay(0L);

    public static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];
    public static final Object[][] EMPTY_OBJECT_ARRAY2 = new Object[0][];
    public static final boolean[] EMPTY_BOOL_ARRAY = new boolean[0];
    public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    public static final short[] EMPTY_SHORT_ARRAY = new short[0];
    public static final int[] EMPTY_INT_ARRAY = new int[0];
    public static final long[] EMPTY_LONG_ARRAY = new long[0];
    public static final float[] EMPTY_FLOAT_ARRAY = new float[0];
    public static final double[] EMPTY_DOUBLE_ARRAY = new double[0];

    public static final ClickHouseValue[] EMPTY_VALUES = new ClickHouseValue[0];

    public static final String EMPTY_ARRAY_EXPR = "[]";
    public static final String EMPTY_MAP_EXPR = "{}";
    public static final String EMPTY_STRING_EXPR = "''";
    public static final String EMPTY_TUPLE_EXPR = "()";

    public static final BigDecimal NANOS = new BigDecimal(BigInteger.TEN.pow(9));

    public static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    public static final DateTimeFormatter TIME_FORMATTER = new DateTimeFormatterBuilder().appendPattern("HH:mm:ss")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).toFormatter();
    public static final DateTimeFormatter DATETIME_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss").appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).toFormatter();

    public static final TimeZone UTC_TIMEZONE = TimeZone.getTimeZone("UTC");
    public static final TimeZone SYS_TIMEZONE = TimeZone.getDefault();
    public static final ZoneId UTC_ZONE = UTC_TIMEZONE.toZoneId();
    public static final ZoneId SYS_ZONE = SYS_TIMEZONE.toZoneId();

    public static final String NULL_EXPR = "NULL";
    public static final String NAN_EXPR = "NaN";
    public static final String INF_EXPR = "Inf";
    public static final String NINF_EXPR = "-Inf";

    public static final String ERROR_INF_OR_NAN = "Infinite or NaN";
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

    private static final byte[] HEX_ARRAY = "0123456789ABCDEF".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] UNHEX_PREFIX = "unhex('".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] UNHEX_SUFFIX = "')".getBytes(StandardCharsets.US_ASCII);

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
     * Converts given character to boolean value.
     *
     * @param value character represents a boolean value
     * @return boolean value
     */
    public static boolean convertToBoolean(char value) {
        // not going to support X/V, ✓/✗ and 是/否 as they're less common
        if (value == '1' || value == 'T' || value == 't' || value == 'Y' || value == 'y') {
            return true;
        } else if (value == '0' || value == 'F' || value == 'f' || value == 'N' || value == 'n') {
            return false;
        } else {
            throw new IllegalArgumentException("Invalid boolean value, please use 1/0, T/F, Y/N");
        }
    }

    /**
     * Converts given string to boolean value.
     *
     * @param value string represents a boolean value
     * @return boolean value
     */
    public static boolean convertToBoolean(String value) {
        if (value == null || value.isEmpty() || "0".equals(value) || "false".equalsIgnoreCase(value)
                || "no".equalsIgnoreCase(value)) {
            return false;
        } else if ("1".equals(value) || "true".equalsIgnoreCase(value) || "yes".equalsIgnoreCase(value)) {
            return true;
        } else {
            throw new IllegalArgumentException("Invalid boolean value, please use 1/0, true/false, yes/no");
        }
    }

    /**
     * Converts given byte array to string in hexadecimal format.
     *
     * @param bytes byte array
     * @return non-null string
     */
    public static String convertToHexString(byte[] bytes) {
        int len = bytes != null ? bytes.length : 0;
        if (len == 0) {
            return "";
        }

        byte[] hexChars = new byte[len * 2];
        for (int i = 0; i < len; i++) {
            int v = bytes[i] & 0xFF;
            int j = i * 2;
            hexChars[j] = HEX_ARRAY[v >>> 4];
            hexChars[j + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars, StandardCharsets.UTF_8);
    }

    /**
     * Converts given byte array to unhex() expression.
     *
     * @param bytes byte array
     * @return non-null expression
     */
    public static String convertToUnhexExpression(byte[] bytes) {
        int len = bytes != null ? bytes.length : 0;
        if (len == 0) {
            return EMPTY_STRING_EXPR;
        }

        int offset = UNHEX_PREFIX.length;
        byte[] hexChars = new byte[len * 2 + offset + UNHEX_SUFFIX.length];
        System.arraycopy(UNHEX_PREFIX, 0, hexChars, 0, offset);
        System.arraycopy(UNHEX_SUFFIX, 0, hexChars, hexChars.length - UNHEX_SUFFIX.length, UNHEX_SUFFIX.length);
        for (int i = 0; i < len; i++) {
            int v = bytes[i] & 0xFF;
            hexChars[offset++] = HEX_ARRAY[v >>> 4];
            hexChars[offset++] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars, StandardCharsets.UTF_8);
    }

    /**
     * Converts big decimal to instant.
     *
     * @param value big decimal
     * @return instant
     */
    public static Instant convertToInstant(BigDecimal value) {
        if (value == null) {
            return null;
        } else if (value.scale() == 0) {
            return Instant.ofEpochSecond(value.longValue());
        } else if (value.signum() >= 0) {
            return Instant.ofEpochSecond(value.longValue(),
                    value.remainder(BigDecimal.ONE).multiply(NANOS).intValue());
        }

        long v = NANOS.add(value.remainder(BigDecimal.ONE).multiply(NANOS)).longValue();
        int nanoSeconds = v < 1000000000L ? (int) v : 0;

        return Instant.ofEpochSecond(value.longValue() - (nanoSeconds > 0 ? 1 : 0), nanoSeconds);
    }

    /**
     * Converts big decimal to date time.
     *
     * @param value big decimal
     * @return date time
     */
    public static LocalDateTime convertToDateTime(BigDecimal value) {
        return value != null ? LocalDateTime.ofInstant(convertToInstant(value), ZoneOffset.UTC) : null;
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

        return convertToInstant(value).atZone(zone != null ? zone : UTC_ZONE);
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

        return convertToInstant(value).atOffset(offset != null ? offset : ZoneOffset.UTC);
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
        } else if (value.isEmpty()) {
            return ClickHouseIpv4Value.DEFAULT;
        }

        try {
            for (InetAddress addr : InetAddress.getAllByName(value)) {
                if (addr instanceof Inet4Address) {
                    return (Inet4Address) addr;
                } else if (addr instanceof Inet6Address) {
                    return convertToIpv4((Inet6Address) addr);
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
        } else if (value.isEmpty()) {
            return ClickHouseIpv6Value.DEFAULT;
        }

        try {
            for (InetAddress addr : InetAddress.getAllByName(value)) {
                if (addr instanceof Inet6Address) {
                    return (Inet6Address) addr;
                } else if (addr instanceof Inet4Address) {
                    return convertToIpv6((Inet4Address) addr);
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
        int len;
        if (value == null) { // should not happen
            return NULL_EXPR;
        } else if ((len = value.length) == 0) {
            return EMPTY_ARRAY_EXPR;
        }

        StringBuilder builder = new StringBuilder(len < 3 ? 16 : 6 * len).append('[').append(value[0]);
        for (int i = 1; i < len; i++) {
            builder.append(',').append(value[i]);
        }
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
        int len;
        if (value == null) { // should not happen
            return NULL_EXPR;
        } else if ((len = value.length) == 0) {
            return EMPTY_ARRAY_EXPR;
        }

        StringBuilder builder = new StringBuilder(len < 3 ? 16 : 6 * len).append('[').append((short) value[0]);
        for (int i = 1; i < len; i++) {
            builder.append(',').append((short) value[i]);
        }
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
        int len = 0;
        if (value == null) { // should not happen
            return NULL_EXPR;
        } else if ((len = value.length) == 0) {
            return EMPTY_ARRAY_EXPR;
        }

        StringBuilder builder = new StringBuilder(len < 4 ? 16 : 4 * len).append('[').append(value[0]);
        for (int i = 1; i < len; i++) {
            builder.append(',').append(value[i]);
        }
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
        int len;
        if (value == null) { // should not happen
            return NULL_EXPR;
        } else if ((len = value.length) == 0) {
            return EMPTY_ARRAY_EXPR;
        }

        StringBuilder builder = new StringBuilder(len < 3 ? 16 : 6 * len).append('[').append(value[0]);
        for (int i = 1; i < len; i++) {
            builder.append(',').append(value[i]);
        }
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
        int len;
        if (value == null) { // should not happen
            return NULL_EXPR;
        } else if ((len = value.length) == 0) {
            return EMPTY_ARRAY_EXPR;
        }

        StringBuilder builder = new StringBuilder(len < 3 ? 16 : 6 * len).append('[').append(value[0]);
        for (int i = 1; i < len; i++) {
            builder.append(',').append(value[i]);
        }
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
        int len;
        if (value == null) { // should not happen
            return NULL_EXPR;
        } else if ((len = value.length) == 0) {
            return EMPTY_ARRAY_EXPR;
        }

        StringBuilder builder = new StringBuilder(len < 3 ? 16 : 6 * len).append('[').append(value[0]);
        for (int i = 1; i < len; i++) {
            builder.append(',').append(value[i]);
        }
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
        int len;
        if (value == null) { // should not happen
            return NULL_EXPR;
        } else if ((len = value.length) == 0) {
            return EMPTY_ARRAY_EXPR;
        }

        StringBuilder builder = new StringBuilder(len < 3 ? 16 : 6 * len).append('[').append(value[0]);
        for (int i = 1; i < len; i++) {
            builder.append(',').append(value[i]);
        }
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
        int len;
        if (value == null) { // should not happen
            return NULL_EXPR;
        } else if ((len = value.length) == 0) {
            return EMPTY_ARRAY_EXPR;
        }

        StringBuilder builder = new StringBuilder(len < 3 ? 16 : 6 * len).append('[').append(value[0]);
        for (int i = 1; i < len; i++) {
            builder.append(',').append(value[i]);
        }
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

        return new UUID(high.longValue(), low.longValue());
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

    static ClickHouseArrayValue<?> fillByteArray(ClickHouseArrayValue<?> array, ClickHouseDataConfig config,
            ClickHouseColumn column, ClickHouseInputStream input, ClickHouseDeserializer deserializer,
            int length) throws IOException {
        byte[] values = new byte[length];
        ClickHouseValue ref = ClickHouseByteValue.ofNull();
        for (int i = 0; i < length; i++) {
            values[i] = deserializer.deserialize(ref, input).asByte();
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
     * Creates multiple values based on given columns.
     *
     * @param config  non-null configuration
     * @param columns non-null columns
     * @return non-null values with default value, either null or empty
     */
    public static ClickHouseValue[] newValues(ClickHouseDataConfig config, List<ClickHouseColumn> columns) {
        if (columns == null || columns.isEmpty()) {
            return EMPTY_VALUES;
        }

        ClickHouseValue[] values = new ClickHouseValue[columns.size()];
        int index = 0;
        for (ClickHouseColumn c : columns) {
            values[index++] = c.newValue(config);
        }
        return values;
    }

    /**
     * Creates multiple values based on given columns.
     *
     * @param config  non-null configuration
     * @param columns non-null columns
     * @return non-null values with default value, either null or empty
     */
    public static ClickHouseValue[] newValues(ClickHouseDataConfig config, ClickHouseColumn[] columns) {
        if (columns == null || columns.length == 0) {
            return EMPTY_VALUES;
        }

        int len = columns.length;
        ClickHouseValue[] values = new ClickHouseValue[len];
        for (int i = 0; i < len; i++) {
            values[i] = columns[i].newValue(config);
        }
        return values;
    }

    private ClickHouseValues() {
    }
}

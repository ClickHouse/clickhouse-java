package com.clickhouse.client.datatypes.readers;

import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.query.NullValueException;
import com.clickhouse.client.testdata.DataTypeGenerator;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static com.clickhouse.client.datatypes.readers.ReaderAccessor.*;

/**
 * Expected result of every {@link ReaderAccessor} for a value of a column.
 *
 * <p>An accessor is a valid conversion for a column when it is listed in {@link #VALID_ACCESSORS} for the column
 * category; any other accessor must throw {@link ClientException}. A valid conversion must be lossless for the
 * value: integer accessors require an integral value in range, {@code getBigDecimal} a finite value,
 * {@code getBoolean} a value of 0 or 1, otherwise {@link ClientException} is expected. {@code getFloat} and
 * {@code getDouble} may round, as documented, but must not overflow. {@code getString} returns the text
 * ClickHouse writes for the value, the same as in the {@code TabSeparated} format but unescaped.
 *
 * <p>For a {@code NULL} value valid accessors return {@code null}, or throw {@link NullValueException} when
 * they return a primitive.
 */
final class ReaderExpectations {

    enum Category {
        INTEGER, FLOAT, DECIMAL, BOOL, ENUM, STRING, FIXED_STRING, DATE, TIME, DATE_TIME, IPV4, IPV6, UUID,
        POINT, RING, POLYGON, MULTI_POLYGON, ARRAY, TUPLE, MAP, NESTED
    }

    private static final Set<ReaderAccessor> NUMBERS = EnumSet.of(STRING, BYTE, SHORT, INTEGER, LONG, FLOAT, DOUBLE,
            BOOLEAN, BIG_INTEGER, BIG_DECIMAL);

    /**
     * Valid conversions, seeded from what the binary readers support today.
     */
    static final Map<Category, Set<ReaderAccessor>> VALID_ACCESSORS = new EnumMap<>(Category.class);

    static {
        VALID_ACCESSORS.put(Category.INTEGER, NUMBERS);
        VALID_ACCESSORS.put(Category.FLOAT, NUMBERS);
        VALID_ACCESSORS.put(Category.DECIMAL, NUMBERS);
        VALID_ACCESSORS.put(Category.BOOL, NUMBERS);
        Set<ReaderAccessor> enums = EnumSet.copyOf(NUMBERS);
        enums.add(ENUM8);
        enums.add(ENUM16);
        VALID_ACCESSORS.put(Category.ENUM, enums);
        Set<ReaderAccessor> strings = EnumSet.copyOf(NUMBERS);
        strings.add(BYTE_ARRAY);
        VALID_ACCESSORS.put(Category.STRING, strings);
        VALID_ACCESSORS.put(Category.FIXED_STRING, EnumSet.of(STRING, BYTE_ARRAY));
        VALID_ACCESSORS.put(Category.DATE, EnumSet.of(STRING, INSTANT, LOCAL_DATE));
        VALID_ACCESSORS.put(Category.TIME, EnumSet.of(STRING, DURATION, TEMPORAL_AMOUNT, INSTANT, LOCAL_DATE_TIME,
                LOCAL_TIME));
        VALID_ACCESSORS.put(Category.DATE_TIME, EnumSet.of(STRING, INSTANT, ZONED_DATE_TIME, OFFSET_DATE_TIME,
                LOCAL_DATE, LOCAL_DATE_TIME, LOCAL_TIME));
        VALID_ACCESSORS.put(Category.IPV4, EnumSet.of(STRING, INET4_ADDRESS, INET6_ADDRESS, BYTE_ARRAY));
        VALID_ACCESSORS.put(Category.IPV6, EnumSet.of(STRING, INET4_ADDRESS, INET6_ADDRESS, BYTE_ARRAY));
        VALID_ACCESSORS.put(Category.UUID, EnumSet.of(STRING, ReaderAccessor.UUID));
        VALID_ACCESSORS.put(Category.POINT, EnumSet.of(STRING, GEO_POINT));
        VALID_ACCESSORS.put(Category.RING, EnumSet.of(STRING, GEO_RING));
        VALID_ACCESSORS.put(Category.POLYGON, EnumSet.of(STRING, GEO_POLYGON));
        VALID_ACCESSORS.put(Category.MULTI_POLYGON, EnumSet.of(STRING, GEO_MULTI_POLYGON));
        // string and primitive arrays are added per element type, see arrayAccessors
        VALID_ACCESSORS.put(Category.ARRAY, EnumSet.of(STRING, LIST, OBJECT_ARRAY));
        VALID_ACCESSORS.put(Category.TUPLE, EnumSet.of(STRING, TUPLE));
        VALID_ACCESSORS.put(Category.MAP, EnumSet.of(STRING));
        VALID_ACCESSORS.put(Category.NESTED, EnumSet.of(STRING, LIST, OBJECT_ARRAY));
    }

    private static final Map<ClickHouseDataType, ReaderAccessor> PRIMITIVE_ARRAY_ACCESSORS =
            new EnumMap<>(ClickHouseDataType.class);

    static {
        PRIMITIVE_ARRAY_ACCESSORS.put(ClickHouseDataType.Int8, BYTE_ARRAY);
        PRIMITIVE_ARRAY_ACCESSORS.put(ClickHouseDataType.Int16, SHORT_ARRAY);
        PRIMITIVE_ARRAY_ACCESSORS.put(ClickHouseDataType.UInt8, SHORT_ARRAY);
        PRIMITIVE_ARRAY_ACCESSORS.put(ClickHouseDataType.Int32, INT_ARRAY);
        PRIMITIVE_ARRAY_ACCESSORS.put(ClickHouseDataType.UInt16, INT_ARRAY);
        PRIMITIVE_ARRAY_ACCESSORS.put(ClickHouseDataType.Int64, LONG_ARRAY);
        PRIMITIVE_ARRAY_ACCESSORS.put(ClickHouseDataType.UInt32, LONG_ARRAY);
        PRIMITIVE_ARRAY_ACCESSORS.put(ClickHouseDataType.Float32, FLOAT_ARRAY);
        PRIMITIVE_ARRAY_ACCESSORS.put(ClickHouseDataType.Float64, DOUBLE_ARRAY);
        PRIMITIVE_ARRAY_ACCESSORS.put(ClickHouseDataType.Bool, BOOLEAN_ARRAY);
    }

    private static final Duration DAY = Duration.ofDays(1);

    private final ClickHouseColumn column;
    private final DataTypeGenerator generator;
    private final Category category;
    private final Set<ReaderAccessor> validAccessors;

    ReaderExpectations(ClickHouseColumn column, DataTypeGenerator generator) {
        this.column = column;
        this.generator = generator;
        this.category = category(column);
        this.validAccessors = EnumSet.copyOf(VALID_ACCESSORS.get(category));
        if (category == Category.ARRAY) {
            validAccessors.addAll(arrayAccessors(column));
        }
    }

    Category getCategory() {
        return category;
    }

    boolean isValid(ReaderAccessor accessor) {
        return validAccessors.contains(accessor);
    }

    Expected expected(ReaderAccessor accessor, Object value) {
        if (!isValid(accessor)) {
            return Expected.exception(ClientException.class);
        } else if (value == null) {
            return accessor.isPrimitive() ? Expected.exception(NullValueException.class) : Expected.value(null);
        }
        switch (accessor) {
            case STRING:
                return Expected.value(text(value));
            case BYTE:
                return integral(value, BigInteger.valueOf(Byte.MIN_VALUE), BigInteger.valueOf(Byte.MAX_VALUE),
                        BigInteger::byteValue);
            case SHORT:
                return integral(value, BigInteger.valueOf(Short.MIN_VALUE), BigInteger.valueOf(Short.MAX_VALUE),
                        BigInteger::shortValue);
            case INTEGER:
                return integral(value, BigInteger.valueOf(Integer.MIN_VALUE), BigInteger.valueOf(Integer.MAX_VALUE),
                        BigInteger::intValue);
            case LONG:
                return integral(value, BigInteger.valueOf(Long.MIN_VALUE), BigInteger.valueOf(Long.MAX_VALUE),
                        BigInteger::longValue);
            case BIG_INTEGER:
                return integral(value, null, null, v -> v);
            case BIG_DECIMAL:
                return bigDecimal(value);
            case FLOAT:
                return floatValue(value);
            case DOUBLE:
                return doubleValue(value);
            case BOOLEAN:
                return booleanValue(value);
            case ENUM8:
                return integral(value, BigInteger.valueOf(Byte.MIN_VALUE), BigInteger.valueOf(Byte.MAX_VALUE),
                        BigInteger::byteValue);
            case ENUM16:
                return integral(value, BigInteger.valueOf(Short.MIN_VALUE), BigInteger.valueOf(Short.MAX_VALUE),
                        BigInteger::shortValue);
            case BYTE_ARRAY:
                return byteArray(value);
            case INSTANT:
                return instant(value);
            case ZONED_DATE_TIME:
                return Expected.value(value);
            case OFFSET_DATE_TIME:
                return Expected.value(((ZonedDateTime) value).toOffsetDateTime());
            case LOCAL_DATE:
                return Expected.value(value instanceof LocalDate ? value : ((ZonedDateTime) value).toLocalDate());
            case LOCAL_DATE_TIME:
                return Expected.value(value instanceof Duration
                        ? LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC).plus((Duration) value)
                        : ((ZonedDateTime) value).toLocalDateTime());
            case LOCAL_TIME:
                return localTime(value);
            case DURATION:
            case TEMPORAL_AMOUNT:
                return Expected.value(value);
            case INET4_ADDRESS:
                return inet4Address((InetAddress) value);
            case INET6_ADDRESS:
                return Expected.value(inet6Address((InetAddress) value));
            case UUID:
            case GEO_POINT:
            case GEO_RING:
            case GEO_POLYGON:
            case GEO_MULTI_POLYGON:
            case LIST:
            case OBJECT_ARRAY:
            case STRING_ARRAY:
            case TUPLE:
            case SHORT_ARRAY:
            case INT_ARRAY:
            case LONG_ARRAY:
            case FLOAT_ARRAY:
            case DOUBLE_ARRAY:
            case BOOLEAN_ARRAY:
                return Expected.value(value);
            default:
                throw new IllegalStateException("No expectation for valid accessor " + accessor);
        }
    }

    static Category category(ClickHouseColumn column) {
        ClickHouseDataType type = column.getDataType();
        switch (type) {
            case Int8:
            case UInt8:
            case Int16:
            case UInt16:
            case Int32:
            case UInt32:
            case Int64:
            case UInt64:
            case Int128:
            case UInt128:
            case Int256:
            case UInt256:
                return Category.INTEGER;
            case Float32:
            case Float64:
                return Category.FLOAT;
            case Decimal:
            case Decimal32:
            case Decimal64:
            case Decimal128:
            case Decimal256:
                return Category.DECIMAL;
            case Bool:
                return Category.BOOL;
            case Enum8:
            case Enum16:
                return Category.ENUM;
            case String:
                return Category.STRING;
            case FixedString:
                return Category.FIXED_STRING;
            case Date:
            case Date32:
                return Category.DATE;
            case Time:
            case Time64:
                return Category.TIME;
            case DateTime:
            case DateTime32:
            case DateTime64:
                return Category.DATE_TIME;
            case IPv4:
                return Category.IPV4;
            case IPv6:
                return Category.IPV6;
            case UUID:
                return Category.UUID;
            case Point:
                return Category.POINT;
            case Ring:
            case LineString:
                return Category.RING;
            case Polygon:
            case MultiLineString:
                return Category.POLYGON;
            case MultiPolygon:
                return Category.MULTI_POLYGON;
            case Array:
                return Category.ARRAY;
            case Tuple:
                return Category.TUPLE;
            case Map:
                return Category.MAP;
            case Nested:
                return Category.NESTED;
            default:
                throw new IllegalArgumentException("Unsupported column type " + column.getOriginalTypeName());
        }
    }

    /**
     * {@code getStringArray} for one-dimensional arrays of strings and enums, a primitive array accessor for
     * one-dimensional arrays of non-nullable numbers of the matching Java type.
     */
    private static Set<ReaderAccessor> arrayAccessors(ClickHouseColumn column) {
        ClickHouseColumn element = column.getArrayBaseColumn();
        if (column.getArrayNestedLevel() != 1) {
            return Collections.emptySet();
        }
        switch (element.getDataType()) {
            case String:
            case FixedString:
            case Enum8:
            case Enum16:
                return EnumSet.of(STRING_ARRAY);
            default:
                ReaderAccessor accessor = PRIMITIVE_ARRAY_ACCESSORS.get(element.getDataType());
                return accessor == null || element.isNullable() ? Collections.emptySet() : EnumSet.of(accessor);
        }
    }

    private String text(Object value) {
        String tsv = generator.toTsv(value);
        switch (category) {
            case POINT:
            case RING:
            case POLYGON:
            case MULTI_POLYGON:
            case ARRAY:
            case TUPLE:
            case MAP:
            case NESTED:
                // composite values are written as is, without escaping
                return new String(tsv.getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8);
            default:
                return new String(unescape(tsv).getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8);
        }
    }

    /**
     * @return exact numeric value, or {@code null} for NaN and infinity
     */
    private BigDecimal number(Object value) {
        if (value instanceof Boolean) {
            return (Boolean) value ? BigDecimal.ONE : BigDecimal.ZERO;
        } else if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        } else if (value instanceof BigInteger) {
            return new BigDecimal((BigInteger) value);
        } else if (value instanceof Float || value instanceof Double) {
            double v = ((Number) value).doubleValue();
            return Double.isFinite(v) ? new BigDecimal(v) : null;
        } else if (value instanceof Number) {
            return BigDecimal.valueOf(((Number) value).longValue());
        } else if (category == Category.ENUM) {
            return BigDecimal.valueOf(column.getEnumConstants().value((String) value));
        } else if (value instanceof String) {
            try {
                return new BigDecimal((String) value);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

    private Expected integral(Object value, BigInteger min, BigInteger max,
            java.util.function.Function<BigInteger, Object> convert) {
        BigDecimal number = number(value);
        if (number == null || number.signum() != 0 && number.stripTrailingZeros().scale() > 0) {
            return Expected.exception(ClientException.class);
        }
        BigInteger integer = number.toBigIntegerExact();
        if (min != null && (integer.compareTo(min) < 0 || integer.compareTo(max) > 0)) {
            return Expected.exception(ClientException.class);
        }
        return Expected.value(convert.apply(integer));
    }

    private Expected bigDecimal(Object value) {
        BigDecimal number = number(value);
        if (number == null) {
            return Expected.exception(ClientException.class);
        } else if (value instanceof Float) {
            // any decimal that converts back to the same float is a lossless representation
            float v = (Float) value;
            return Expected.matching("BigDecimal equal to float " + v,
                    a -> a instanceof BigDecimal && ((BigDecimal) a).floatValue() == v);
        } else if (value instanceof Double) {
            double v = (Double) value;
            return Expected.matching("BigDecimal equal to double " + v,
                    a -> a instanceof BigDecimal && ((BigDecimal) a).doubleValue() == v);
        }
        return Expected.value(number);
    }

    private Expected floatValue(Object value) {
        if (value instanceof Float || value instanceof Double) {
            float v = ((Number) value).floatValue();
            return Float.isInfinite(v) && !Double.isInfinite(((Number) value).doubleValue())
                    ? Expected.exception(ClientException.class) : Expected.value(v);
        }
        BigDecimal number = number(value);
        if (number == null) {
            return Expected.exception(ClientException.class);
        }
        float v = number.floatValue();
        return Float.isInfinite(v) ? Expected.exception(ClientException.class) : Expected.value(v);
    }

    private Expected doubleValue(Object value) {
        if (value instanceof Float || value instanceof Double) {
            return Expected.value(((Number) value).doubleValue());
        }
        BigDecimal number = number(value);
        if (number == null) {
            return Expected.exception(ClientException.class);
        }
        double v = number.doubleValue();
        return Double.isInfinite(v) ? Expected.exception(ClientException.class) : Expected.value(v);
    }

    private Expected booleanValue(Object value) {
        if (value instanceof Boolean) {
            return Expected.value(value);
        } else if (value instanceof String && category == Category.STRING) {
            return "true".equals(value) || "false".equals(value) ? Expected.value(Boolean.valueOf((String) value))
                    : Expected.exception(ClientException.class);
        }
        BigDecimal number = number(value);
        if (number != null && number.compareTo(BigDecimal.ZERO) == 0) {
            return Expected.value(false);
        } else if (number != null && number.compareTo(BigDecimal.ONE) == 0) {
            return Expected.value(true);
        }
        return Expected.exception(ClientException.class);
    }

    private Expected byteArray(Object value) {
        if (value instanceof byte[]) {
            // binary_string_support is disabled by default, so strings are decoded as UTF-8 and invalid
            // sequences are replaced as documented
            return Expected.value(new String((byte[]) value, StandardCharsets.UTF_8).getBytes(StandardCharsets.UTF_8));
        } else if (value instanceof String) {
            return Expected.value(((String) value).getBytes(StandardCharsets.UTF_8));
        } else if (value instanceof InetAddress) {
            return Expected.value(((InetAddress) value).getAddress());
        }
        return Expected.value(value);
    }

    private Expected instant(Object value) {
        if (value instanceof LocalDate) {
            return Expected.value(((LocalDate) value).atStartOfDay(ZoneOffset.UTC).toInstant());
        } else if (value instanceof Duration) {
            return Expected.value(Instant.EPOCH.plus((Duration) value));
        }
        return Expected.value(((ZonedDateTime) value).toInstant());
    }

    private Expected localTime(Object value) {
        if (value instanceof Duration) {
            Duration duration = (Duration) value;
            return duration.isNegative() || duration.compareTo(DAY) >= 0 ? Expected.exception(ClientException.class)
                    : Expected.value(LocalTime.ofNanoOfDay(duration.toNanos()));
        }
        return Expected.value(((ZonedDateTime) value).toLocalTime());
    }

    private static Expected inet4Address(InetAddress value) {
        if (value instanceof Inet4Address) {
            return Expected.value(value);
        }
        byte[] bytes = value.getAddress();
        for (int i = 0; i < 10; i++) {
            if (bytes[i] != 0) {
                return Expected.exception(ClientException.class);
            }
        }
        if (bytes[10] != (byte) 0xFF || bytes[11] != (byte) 0xFF) {
            return Expected.exception(ClientException.class);
        }
        return Expected.value(address(Arrays.copyOfRange(bytes, 12, 16)));
    }

    private static Inet6Address inet6Address(InetAddress value) {
        if (value instanceof Inet6Address) {
            return (Inet6Address) value;
        }
        byte[] bytes = new byte[16];
        bytes[10] = (byte) 0xFF;
        bytes[11] = (byte) 0xFF;
        System.arraycopy(value.getAddress(), 0, bytes, 12, 4);
        try {
            return Inet6Address.getByAddress(null, bytes, -1);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static InetAddress address(byte[] bytes) {
        try {
            return InetAddress.getByAddress(bytes);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Reverses the escaping of {@code TabSeparated} values.
     */
    static String unescape(String value) {
        StringBuilder text = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c != '\\' || i + 1 == value.length()) {
                text.append(c);
                continue;
            }
            char next = value.charAt(++i);
            switch (next) {
                case 'b':
                    text.append('\b');
                    break;
                case 'f':
                    text.append('\f');
                    break;
                case 'n':
                    text.append('\n');
                    break;
                case 'r':
                    text.append('\r');
                    break;
                case 't':
                    text.append('\t');
                    break;
                case '0':
                    text.append('\0');
                    break;
                default:
                    text.append(next);
            }
        }
        return text.toString();
    }

    /**
     * Expected outcome of an accessor call: a value, a value matching a condition, or an exception.
     */
    static final class Expected {
        private final Object value;
        private final Predicate<Object> matcher;
        private final String description;
        private final Class<? extends Exception> exception;

        private Expected(Object value, Predicate<Object> matcher, String description,
                Class<? extends Exception> exception) {
            this.value = value;
            this.matcher = matcher;
            this.description = description;
            this.exception = exception;
        }

        static Expected value(Object value) {
            Object normalized = ReaderValues.normalize(value);
            return new Expected(value, actual -> ReaderValues.deepEquals(normalized, ReaderValues.normalize(actual)),
                    null, null);
        }

        static Expected matching(String description, Predicate<Object> matcher) {
            return new Expected(null, matcher, description, null);
        }

        static Expected exception(Class<? extends Exception> exception) {
            return new Expected(null, null, null, exception);
        }

        boolean isException() {
            return exception != null;
        }

        Class<? extends Exception> getException() {
            return exception;
        }

        boolean matches(Object actual) {
            return matcher.test(actual);
        }

        @Override
        public String toString() {
            if (exception != null) {
                return exception.getSimpleName();
            }
            return description != null ? description : ReaderValues.describe(value);
        }
    }
}

package com.clickhouse.data;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;

import com.clickhouse.data.value.ClickHouseGeoMultiPolygonValue;
import com.clickhouse.data.value.ClickHouseGeoPointValue;
import com.clickhouse.data.value.ClickHouseGeoPolygonValue;
import com.clickhouse.data.value.ClickHouseGeoRingValue;
import com.clickhouse.data.value.UnsignedByte;
import com.clickhouse.data.value.UnsignedInteger;
import com.clickhouse.data.value.UnsignedLong;
import com.clickhouse.data.value.UnsignedShort;

/**
 * Basic ClickHouse data types.
 *
 * <p>
 * This list is based on the list of data type families returned by
 * {@code SELECT * FROM system.data_type_families}
 *
 * <p>
 * {@code LowCardinality} and {@code Nullable} are technically data types in
 * ClickHouse, but for the sake of this driver, we treat these data types as
 * modifiers for the underlying base data types.
 */
@SuppressWarnings("squid:S115")
public enum ClickHouseDataType {
    Bool(Boolean.class, false, false, true, 1, 1, 0, 0, 0, false,0x2D, "BOOLEAN"),
    Date(LocalDate.class, false, false, false, 2, 10, 0, 0, 0, false, 0x0F),
    Date32(LocalDate.class, false, false, false, 4, 10, 0, 0, 0, false, 0x10),
    DateTime(LocalDateTime.class, true, false, false, 0, 29, 0, 0, 9, false, 0x11, "TIMESTAMP"),
    DateTime32(LocalDateTime.class, true, false, false, 4, 19, 0, 0, 0, false, 0x12),
    DateTime64(LocalDateTime.class, true, false, false, 8, 29, 3, 0, 9, false, 0x14), // we always write timezone as argument
    Enum(String.class, true, true, false, 0, 0, 0, 0, 0, false),
    Enum8(String.class, true, true, false, 1, 0, 0, 0, 0, false, 0x17, "ENUM"),
    Enum16(String.class, true, true, false, 2, 0, 0, 0, 0, false, 0x18),
    FixedString(String.class, true, true, false, 0, 0, 0, 0, 0, false, 0x16, "BINARY"),
    Int8(Byte.class, false, true, true, 1, 3, 0, 0, 0, false, 0x07,"BYTE", "INT1", "INT1 SIGNED", "TINYINT",
            "TINYINT SIGNED"),
    UInt8(UnsignedByte.class, false, true, false, 1, 3, 0, 0, 0, false,0x01, "INT1 UNSIGNED", "TINYINT UNSIGNED"),
    Int16(Short.class, false, true, true, 2, 5, 0, 0, 0, false, 0x08,"SMALLINT", "SMALLINT SIGNED"),
    UInt16(UnsignedShort.class, false, true, false, 2, 5, 0, 0, 0, false, 0x02,"SMALLINT UNSIGNED", "YEAR"),
    Int32(Integer.class, false, true, true, 4, 10, 0, 0, 0, false, 0x09, "INT", "INT SIGNED", "INTEGER", "INTEGER SIGNED",
            "MEDIUMINT", "MEDIUMINT SIGNED"),
    // https://docs.oracle.com/javase/tutorial/java/nutsandbolts/datatypes.html#PageTitle
    UInt32(UnsignedInteger.class, false, true, false, 4, 10, 0, 0, 0, false, 0x03, "INT UNSIGNED", "INTEGER UNSIGNED",
            "MEDIUMINT UNSIGNED"),
    Int64(Long.class, false, true, true, 8, 19, 0, 0, 0, false, 0x0A,"BIGINT", "BIGINT SIGNED", "TIME"),
    IntervalYear(Long.class, false, true, true, 8, 19, 0, 0, 0, false, 0x22),
    IntervalQuarter(Long.class, false, true, true, 8, 19, 0, 0, 0, false, 0x22),
    IntervalMonth(Long.class, false, true, true, 8, 19, 0, 0, 0, false, 0x22),
    IntervalWeek(Long.class, false, true, true, 8, 19, 0, 0, 0, false, 0x22),
    IntervalDay(Long.class, false, true, true, 8, 19, 0, 0, 0, false, 0x22),
    IntervalHour(Long.class, false, true, true, 8, 19, 0, 0, 0, false, 0x22),
    IntervalMinute(Long.class, false, true, true, 8, 19, 0, 0, 0, false, 0x22),
    IntervalSecond(Long.class, false, true, true, 8, 19, 0, 0, 0, false, 0x22),
    IntervalMicrosecond(Long.class, false, true, true, 8, 19, 0, 0, 0, false, 0x22),
    IntervalMillisecond(Long.class, false, true, true, 8, 19, 0, 0, 0, false, 0x22),
    IntervalNanosecond(Long.class, false, true, true, 8, 19, 0, 0, 0, false, 0x22),
    UInt64(UnsignedLong.class, false, true, false, 8, 20, 0, 0, 0, false, 0x04, "BIGINT UNSIGNED", "BIT", "SET"),
    Int128(BigInteger.class, false, true, true, 16, 39, 0, 0, 0, false, 0x0B),
    UInt128(BigInteger.class, false, true, false, 16, 39, 0, 0, 0, false, 0x05),
    Int256(BigInteger.class, false, true, true, 32, 77, 0, 0, 0, false, 0x0C),
    UInt256(BigInteger.class, false, true, false, 32, 78, 0, 0, 0, false, 0x06),
    Decimal(BigDecimal.class, true, false, true, 0, 76, 0, 0, 76, false, "DEC", "FIXED", "NUMERIC"),
    Decimal32(BigDecimal.class, true, false, true, 4, 9, 9, 0, 9, false, 0x19),
    Decimal64(BigDecimal.class, true, false, true, 8, 18, 18, 0, 18, false, 0x1A),
    Decimal128(BigDecimal.class, true, false, true, 16, 38, 38, 0, 38, false, 0x1B),
    Decimal256(BigDecimal.class, true, false, true, 32, 76, 20, 0, 76, false, 0x1C),

    BFloat16(Float.class, false, true, true, 2, 3, 0, 0, 16, false, 0x31),
    Float32(Float.class, false, true, true, 4, 12, 0, 0, 38, false, 0x0D, "FLOAT", "REAL", "SINGLE"),
    Float64(Double.class, false, true, true, 8, 22, 0, 0, 308, false, 0x0E, "DOUBLE", "DOUBLE PRECISION"),
    IPv4(Inet4Address.class, false, true, false, 4, 10, 0, 0, 0, false, 0x28, "INET4"),
    IPv6(Inet6Address.class, false, true, false, 16, 39, 0, 0, 0, false, 0x29, "INET6"),
    UUID(UUID.class, false, true, false, 16, 69, 0, 0, 0, false, 0x1D),
    Point(Object.class, false, true, true, 33, 0, 0, 0, 0, true, 0x2C), // same as Tuple(Float64, Float64)
    Polygon(Object.class, false, true, true, 0, 0, 0, 0, 0, true, 0x2C), // same as Array(Ring)
    MultiPolygon(Object.class, false, true, true, 0, 0, 0, 0, 0, true, 0x2C), // same as Array(Polygon)
    Ring(Object.class, false, true, true, 0, 0, 0, 0, 0, true, 0x2C), // same as Array(Point)
    LineString( Object.class, false, true, true, 0, 0, 0, 0, 0, true, 0x2C), // same as Array(Point)
    MultiLineString(Object.class, false, true, true, 0, 0, 0, 0, 0, true, 0x2C), // same as Array(Ring)

    JSON(Object.class, false, false, false, 0, 0, 0, 0, 0, true, 0x30),
    @Deprecated
    Object(Object.class, true, true, false, 0, 0, 0, 0, 0, true),
    String(String.class, false, true, false, 0, 0, 0, 0, 0, false, 0x15, "BINARY LARGE OBJECT", "BINARY VARYING", "BLOB",
            "BYTEA", "CHAR", "CHAR LARGE OBJECT", "CHAR VARYING", "CHARACTER", "CHARACTER LARGE OBJECT",
            "CHARACTER VARYING", "CLOB", "GEOMETRY", "LONGBLOB", "LONGTEXT", "MEDIUMBLOB", "MEDIUMTEXT",
            "NATIONAL CHAR", "NATIONAL CHAR VARYING", "NATIONAL CHARACTER", "NATIONAL CHARACTER LARGE OBJECT",
            "NATIONAL CHARACTER VARYING", "NCHAR", "NCHAR LARGE OBJECT", "NCHAR VARYING", "NVARCHAR", "TEXT",
            "TINYBLOB", "TINYTEXT", "VARBINARY", "VARCHAR", "VARCHAR2"),
    Array(Object.class, true, true, false, 0, 0, 0, 0, 0, true, 0x1E),
    Map(Map.class, true, true, false, 0, 0, 0, 0, 0, true, 0x27),
    Nested(Object.class, true, true, false, 0, 0, 0, 0, 0, true, 0x2F),
    Tuple(List.class, true, true, false, 0, 0, 0, 0, 0, true, 0x1F),
    Nothing(Object.class, false, true, false, 0, 0, 0, 0, 0, true, 0x00),
    LowCardinality(Object.class, true, true, false, 0, 0, 0, 0, 0, true, 0x26),
    Nullable( Object.class, true, true, false, 0, 0, 0, 0, 0, true, 0x23),
    SimpleAggregateFunction(String.class, true, true, false, 0, 0, 0, 0, 0, false, 0x2E),
    // implementation-defined intermediate state
    AggregateFunction(String.class, true, true, false, 0, 0, 0, 0, 0, true),
    Variant(List.class, true, true, false, 0, 0, 0, 0, 0, true, 0x2A),
    Dynamic(Object.class, true, true, false, 0, 0, 0, 0, 0, true, 0x2B),
    ;

    public static final List<ClickHouseDataType> ORDERED_BY_RANGE_INT_TYPES =
            Collections.unmodifiableList(Arrays.asList(
                    Int8, UInt8, Int16, UInt16, Int32, UInt32, Int64, UInt64, Int128, UInt128, Int256, UInt256
            ));

    public static final List<ClickHouseDataType> ORDERED_BY_RANGE_DECIMAL_TYPES =
            Collections.unmodifiableList(Arrays.asList(
                    Float32, Float64, Decimal32, Decimal64, Decimal128, Decimal256, Decimal
            ));

    public static Map<Class<?>, Integer> buildVariantMapping(List<ClickHouseDataType> variantDataTypes) {
        Map<Class<?>, Integer> variantMapping = new HashMap<>();

        TreeMap<ClickHouseDataType, Integer> intTypesMappings = new TreeMap<>(Comparator.comparingInt(ORDERED_BY_RANGE_INT_TYPES::indexOf));
        TreeMap<ClickHouseDataType, Integer> decTypesMappings = new TreeMap<>(Comparator.comparingInt(ORDERED_BY_RANGE_DECIMAL_TYPES::indexOf));

        for (int ordNum = 0; ordNum < variantDataTypes.size(); ordNum++) {
            ClickHouseDataType dataType = variantDataTypes.get(ordNum);
            Set<Class<?>> classSet = DATA_TYPE_TO_CLASS.get(dataType);

            final int finalOrdNum = ordNum;
            if (classSet != null) {
                if (ORDERED_BY_RANGE_INT_TYPES.contains(dataType)) {
                    intTypesMappings.put(dataType, ordNum);
                } else if (ORDERED_BY_RANGE_DECIMAL_TYPES.contains(dataType)) {
                    decTypesMappings.put(dataType, ordNum);
                } else {
                    classSet.forEach(c -> variantMapping.put(c, finalOrdNum));
                }
            }
        }

        // add integers
        for (java.util.Map.Entry<ClickHouseDataType, Integer> entry : intTypesMappings.entrySet()) {
            DATA_TYPE_TO_CLASS.get(entry.getKey()).forEach(c -> variantMapping.put(c, entry.getValue()));
        }
        // add decimals
        for (java.util.Map.Entry<ClickHouseDataType, Integer> entry : decTypesMappings.entrySet()) {
            DATA_TYPE_TO_CLASS.get(entry.getKey()).forEach(c -> variantMapping.put(c, entry.getValue()));
        }

        return variantMapping;
    }

    static final Map<ClickHouseDataType, Set<Class<?>>> DATA_TYPE_TO_CLASS = dataTypeClassMap();
    static Map<ClickHouseDataType, Set<Class<?>>> dataTypeClassMap() {
        Map<ClickHouseDataType, Set<Class<?>>> map = new HashMap<>();

        // We allow to write short to UInt8 even it may not fit. It is done because we have to allow users to utilize UInt* data types.
        List<Class<?>> allNumberClassesOrderedBySize = Arrays.asList(byte.class, Byte.class, short.class, Short.class, int.class, Integer.class, long.class, Long.class, BigInteger.class);
        Set<Class<?>> setOfAllNumberClasses = Collections.unmodifiableSet(new HashSet<>(allNumberClassesOrderedBySize));
        map.put(UInt256, setOfAllNumberClasses);
        map.put(Int256, setOfAllNumberClasses);
        map.put(UInt128, setOfAllNumberClasses);
        map.put(Int128, setOfAllNumberClasses);
        map.put(UInt64, setOfAllNumberClasses);

        map.put(Int64, setOf(byte.class, Byte.class, short.class, Short.class, int.class, Integer.class, long.class, Long.class));
        map.put(UInt32, setOf(byte.class, Byte.class, short.class, Short.class, int.class, Integer.class, long.class, Long.class ));
        map.put(Int32, setOf(byte.class, Byte.class, short.class, Short.class, int.class, Integer.class));
        map.put(UInt16, setOf(byte.class, Byte.class, short.class, Short.class, int.class, Integer.class));
        map.put(Int16, setOf(byte.class, Byte.class, short.class, Short.class));
        map.put(UInt8, setOf(byte.class, Byte.class, short.class, Short.class));
        map.put(Int8, setOf(byte.class, Byte.class));

        map.put(Bool, setOf(boolean.class, Boolean.class));
        map.put(String, setOf(String.class));
        map.put(Float64, setOf(float.class, Float.class, double.class, Double.class));
        map.put(Float32, setOf(float.class, Float.class));
        map.put(Decimal, setOf(float.class, Float.class, double.class, Double.class, BigDecimal.class));
        map.put(Decimal256, setOf(float.class, Float.class, double.class, Double.class, BigDecimal.class));
        map.put(Decimal128, setOf(float.class, Float.class, double.class, Double.class, BigDecimal.class));
        map.put(Decimal64, setOf(float.class, Float.class, double.class, Double.class));
        map.put(Decimal32, setOf(float.class, Float.class));

        map.put(IPv4, setOf(Inet4Address.class));
        map.put(IPv6, setOf(Inet6Address.class));
        map.put(UUID, setOf(java.util.UUID.class));

        map.put(Point, setOf(double[].class, ClickHouseGeoPointValue.class));
        map.put(Ring, setOf(double[][].class, ClickHouseGeoRingValue.class));
        map.put(Polygon, setOf(double[][][].class, ClickHouseGeoPolygonValue.class));
        map.put(MultiPolygon, setOf(double[][][][].class, ClickHouseGeoMultiPolygonValue.class));

        map.put(Date, setOf(LocalDateTime.class, LocalDate.class, ZonedDateTime.class));
        map.put(Date32, setOf(LocalDateTime.class, LocalDate.class, ZonedDateTime.class));
        map.put(DateTime64, setOf(LocalDateTime.class, ZonedDateTime.class));
        map.put(DateTime32, setOf(LocalDateTime.class, ZonedDateTime.class));
        map.put(DateTime, setOf(LocalDateTime.class, ZonedDateTime.class));

        map.put(Enum8, setOf(java.lang.String.class,byte.class, Byte.class, short.class, Short.class, int.class, Integer.class, long.class, Long.class));
        map.put(Enum16, setOf(java.lang.String.class,byte.class, Byte.class, short.class, Short.class, int.class, Integer.class, long.class, Long.class));
        map.put(Array, setOf(List.class, Object[].class, byte[].class, short[].class, int[].class, long[].class, boolean[].class));

        Set<Class<?>> dateIntervalClasses = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(Period.class, Duration.class, byte.class, Byte.class, short.class, Short.class, int.class, Integer.class, long.class, Long.class, BigInteger.class)));
        Set<Class<?>> timeIntervalClasses = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(Duration.class, byte.class, Byte.class, short.class, Short.class, int.class, Integer.class, long.class, Long.class, BigInteger.class)));
        map.put(IntervalYear, dateIntervalClasses);
        map.put(IntervalQuarter, dateIntervalClasses);
        map.put(IntervalMonth, dateIntervalClasses);
        map.put(IntervalWeek, dateIntervalClasses);
        map.put(IntervalDay, dateIntervalClasses);
        map.put(IntervalHour, timeIntervalClasses);
        map.put(IntervalMinute, timeIntervalClasses);
        map.put(IntervalSecond, timeIntervalClasses);
        map.put(IntervalMillisecond, timeIntervalClasses);
        map.put(IntervalMicrosecond, timeIntervalClasses);
        map.put(IntervalNanosecond, timeIntervalClasses);

        return map;
    }

    private static Set<Class<?>> setOf(Class<?>... args) {
        return Collections.unmodifiableSet(new HashSet<>(Arrays.stream(args).collect(Collectors.toList())));
    }

    public static final byte INTERVAL_BIN_TAG = 0x22;

    public static final byte NULLABLE_BIN_TAG = 0x23;

    public static final byte LOW_CARDINALITY_BIN_TAG = 0x26;

    public static final byte SET_BIN_TAG = 0x21;

    public static final byte CUSTOM_TYPE_BIN_TAG = 0x2C;

    public static final byte TUPLE_WITHOUT_NAMES_BIN_TAG = 0x1F;

    public static final byte TUPLE_WITH_NAMES_BIN_TAG = 0x20;

    public enum IntervalKind {
        Nanosecond(IntervalNanosecond, ChronoUnit.NANOS, 0x00),
        Microsecond(IntervalMicrosecond, ChronoUnit.MICROS, 0x01),

        Millisecond(IntervalMillisecond, ChronoUnit.MILLIS, 0x02),

        Second(IntervalSecond,  ChronoUnit.SECONDS, 0x03),

        Minute(IntervalMinute, ChronoUnit.MINUTES, 0x04),

        Hour(IntervalHour, ChronoUnit.HOURS, 0x05),

        Day(IntervalDay, ChronoUnit.DAYS, 0x06),

        Week(IntervalWeek, ChronoUnit.WEEKS, 0x07),

        Month(IntervalMonth, ChronoUnit.MONTHS, 0x08),

        Quarter(IntervalQuarter, null, 0x09),

        Year(IntervalYear, ChronoUnit.YEARS, 0x1A) // why 1A ?

        ;

        private ClickHouseDataType intervalType;

        private TemporalUnit temporalUnit;

        byte tag;
        IntervalKind(ClickHouseDataType clickHouseDataType, TemporalUnit temporalUnit, int tag) {
            this.intervalType = clickHouseDataType;
            this.tag = (byte) tag;
            this.temporalUnit = temporalUnit;
        }

        public ClickHouseDataType getIntervalType() {
            return intervalType;
        }

        public byte getTag() {
            return tag;
        }

        public TemporalUnit getTemporalUnit() { return temporalUnit; }
    }


    /**
     * Immutable set(sorted) for all aliases.
     */
    public static final Set<String> allAliases;

    /**
     * Immutable mapping between name and type.
     */
    public static final Map<String, ClickHouseDataType> name2type;

    public static final Map<Byte, ClickHouseDataType> binTag2Type;

    public static final Map<Byte, ClickHouseDataType> intervalKind2Type;

    public static final Map<ClickHouseDataType, ClickHouseDataType.IntervalKind> intervalType2Kind;

    static {
        Set<String> set = new TreeSet<>();
        Map<String, ClickHouseDataType> map = new HashMap<>();
        String errorMsg = "[%s] is used by type [%s]";
        ClickHouseDataType used = null;
        for (ClickHouseDataType t : ClickHouseDataType.values()) {
            String name = t.name();
            if (!t.isCaseSensitive()) {
                name = name.toUpperCase();
            }
            used = map.put(name, t);
            if (used != null) {
                throw new IllegalStateException(java.lang.String.format(Locale.ROOT, errorMsg, name, used.name()));
            }

            // aliases are all case-insensitive
            for (String alias : t.aliases) {
                String aliasInUpperCase = alias.toUpperCase();
                set.add(aliasInUpperCase);
                used = map.put(aliasInUpperCase, t);
                if (used != null) {
                    throw new IllegalStateException(java.lang.String.format(Locale.ROOT, errorMsg, alias, used.name()));
                }
            }
        }

        allAliases = Collections.unmodifiableSet(set);
        name2type = Collections.unmodifiableMap(map);

        Map<Byte, ClickHouseDataType> tmpbinTag2Type = new HashMap<>();
        for (ClickHouseDataType type : ClickHouseDataType.values()) {
            tmpbinTag2Type.put((byte) type.getBinTag(), type);
        }
        binTag2Type = Collections.unmodifiableMap(tmpbinTag2Type);

        Map<Byte, ClickHouseDataType> tmpIntervalKind2Type = new HashMap<>();
        Map<ClickHouseDataType, ClickHouseDataType.IntervalKind > tmpIntervalType2Kind = new HashMap<>();
        for (IntervalKind kind : IntervalKind.values()) {
            tmpIntervalKind2Type.put(kind.getTag(), kind.getIntervalType());
            tmpIntervalType2Kind.put(kind.getIntervalType(), kind);
        }
        intervalKind2Type = Collections.unmodifiableMap(tmpIntervalKind2Type);
        intervalType2Kind = Collections.unmodifiableMap(tmpIntervalType2Kind);
    }

    /**
     * Checks if the given type name is an alias or not.
     *
     * @param typeName type name
     * @return true if the type name is an alias; false otherwise
     */
    public static boolean isAlias(String typeName) {
        return typeName != null && !typeName.isEmpty() && allAliases.contains(typeName.trim());
    }

    /**
     * Finds list of matched aliases and/or types based on given part.
     *
     * @param part prefix, alias or type name
     * @return list of matched aliases and/or types
     */
    @SuppressWarnings("squid:S3776")
    public static List<String> match(String part) {
        List<String> types = new LinkedList<>();

        for (ClickHouseDataType t : values()) {
            if (t.isCaseSensitive()) {
                if (t.name().equals(part)) {
                    types.add(t.name());
                    break;
                }
            } else {
                if (t.name().equalsIgnoreCase(part)) {
                    types.add(part);
                    break;
                }
            }
        }

        if (types.isEmpty()) {
            part = part.toUpperCase();
            String prefix = part + ' ';
            for (String alias : allAliases) {
                if ((alias.length() == part.length() && alias.equals(part))
                        || (alias.length() > part.length() && alias.startsWith(prefix))) {
                    types.add(alias);
                }
            }
        }

        return types;
    }

    /**
     * Checks if any alias uses the given prefixes.
     *
     * @param prefixes prefixes to check
     * @return true if any alias using the given prefixes; false otherwise
     */
    public static boolean mayStartWith(String... prefixes) {
        if (prefixes == null || prefixes.length == 0) {
            return false;
        }

        StringBuilder builder = new StringBuilder();
        for (int i = 0, len = prefixes.length; i < len; i++) {
            builder.append(prefixes[i].toUpperCase()).append(' ');
        }
        String prefix = builder.toString();
        builder.setLength(builder.length() - 1);
        String typeName = builder.toString();
        for (String alias : allAliases) {
            if (alias.startsWith(prefix) || alias.equals(typeName)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Converts given type name to corresponding data type.
     *
     * @param typeName non-empty type name
     * @return data type
     */
    public static ClickHouseDataType of(String typeName) {
        if (typeName == null || (typeName = typeName.trim()).isEmpty()) { // NOSONAR
            throw new IllegalArgumentException("Non-empty typeName is required");
        }

        ClickHouseDataType type = name2type.get(typeName);
        if (type == null) {
            type = name2type.get(typeName.toUpperCase()); // case-insensitive or just an alias
        }

        if (type == null) {
            throw new IllegalArgumentException("Unknown data type: " + typeName);
        }
        return type;
    }

    /**
     * Converts given Java class to wrapper object(e.g. {@code int.class} to
     * {@code Integer.class}) if applicable.
     *
     * @param javaClass Java class
     * @return wrapper object
     */
    public static Class<?> toObjectType(Class<?> javaClass) {
        if (boolean.class == javaClass) {
            javaClass = Boolean.class;
        } else if (byte.class == javaClass) {
            javaClass = Byte.class;
        } else if (int.class == javaClass) {
            javaClass = Integer.class;
        } else if (long.class == javaClass) {
            javaClass = Long.class;
        } else if (short.class == javaClass || char.class == javaClass || Character.class == javaClass) {
            javaClass = Short.class;
        } else if (float.class == javaClass) {
            javaClass = Float.class;
        } else if (double.class == javaClass) {
            javaClass = Double.class;
        } else if (javaClass == null) {
            javaClass = Object.class;
        }

        return javaClass;
    }

    /**
     * Converts given Java class to wider wrapper object(e.g. {@code int.class} to
     * {@code Long.class}) if applicable.
     *
     * @param javaClass Java class
     * @return wrapper object
     */
    public static Class<?> toWiderObjectType(Class<?> javaClass) {
        if (boolean.class == javaClass) {
            javaClass = Boolean.class;
        } else if (byte.class == javaClass || Byte.class == javaClass || UnsignedByte.class == javaClass) {
            javaClass = Short.class;
        } else if (int.class == javaClass || Integer.class == javaClass || UnsignedInteger.class == javaClass) {
            javaClass = Long.class;
        } else if (long.class == javaClass || Long.class == javaClass) {
            javaClass = UnsignedLong.class;
        } else if (short.class == javaClass || Short.class == javaClass || UnsignedShort.class == javaClass
                || char.class == javaClass || Character.class == javaClass) {
            javaClass = Integer.class;
        } else if (float.class == javaClass) {
            javaClass = Float.class;
        } else if (double.class == javaClass) {
            javaClass = Double.class;
        } else if (javaClass == null) {
            javaClass = Object.class;
        }

        return javaClass;
    }

    /**
     * Converts given Java class to primitive types(e.g. {@code Integer.class} to
     * {@code int.class}) if applicable.
     *
     * @param javaClass Java class
     * @return primitive type
     */
    public static Class<?> toPrimitiveType(Class<?> javaClass) {
        if (Boolean.class == javaClass) {
            javaClass = boolean.class;
        } else if (Byte.class == javaClass || UnsignedByte.class == javaClass) {
            javaClass = byte.class;
        } else if (Integer.class == javaClass || UnsignedInteger.class == javaClass) {
            javaClass = int.class;
        } else if (Long.class == javaClass || UnsignedLong.class == javaClass) {
            javaClass = long.class;
        } else if (Short.class == javaClass || UnsignedShort.class == javaClass || Character.class == javaClass
                || char.class == javaClass) {
            javaClass = short.class;
        } else if (Float.class == javaClass) {
            javaClass = float.class;
        } else if (Double.class == javaClass) {
            javaClass = double.class;
        } else if (javaClass == null) {
            javaClass = Object.class;
        }

        return javaClass;
    }

    /**
     * Converts given Java class to wider primitive types(e.g. {@code Integer.class}
     * to {@code long.class}) if applicable.
     *
     * @param javaClass Java class
     * @return primitive type
     */
    public static Class<?> toWiderPrimitiveType(Class<?> javaClass) {
        if (Boolean.class == javaClass || boolean.class == javaClass) {
            javaClass = boolean.class;
        } else if (Byte.class == javaClass || UnsignedByte.class == javaClass || byte.class == javaClass) {
            javaClass = short.class;
        } else if (Integer.class == javaClass || UnsignedInteger.class == javaClass || int.class == javaClass
                || Long.class == javaClass || UnsignedLong.class == javaClass) {
            javaClass = long.class;
        } else if (Short.class == javaClass || UnsignedShort.class == javaClass || short.class == javaClass
                || Character.class == javaClass || char.class == javaClass) {
            javaClass = int.class;
        } else if (Float.class == javaClass) {
            javaClass = float.class;
        } else if (Double.class == javaClass) {
            javaClass = double.class;
        } else if (javaClass == null) {
            javaClass = Object.class;
        }

        return javaClass;
    }

    private final Class<?> objectType;
    private final Class<?> widerObjectType;
    private final Class<?> primitiveType;
    private final Class<?> widerPrimitiveType;
    private final boolean parameter;
    private final boolean caseSensitive;
    private final boolean signed;
    private final List<String> aliases;
    private final int byteLength;
    private final int maxPrecision;
    private final int defaultScale;
    private final int minScale;
    private final int maxScale;
    private final boolean nestedType;

    private final byte binTag;

    /**
     * Default constructor.
     *
     * @param javaClass     Java class
     * @param parameter     whether supports parameter
     * @param caseSensitive whether the type is case sensitive
     * @param signed        whether it's signed data type
     * @param byteLength    length in byte
     * @param maxPrecision  maximum precision supported
     * @param defaultScale  default scale
     * @param minScale      minimum scale supported
     * @param maxScale      maximum scale supported
     * @param nestedType    whether it's a nested data type
     * @param aliases       list of aliases
     */
    @SuppressWarnings("squid:S107")
    ClickHouseDataType(Class<?> javaClass, boolean parameter, boolean caseSensitive, boolean signed, int byteLength,
            int maxPrecision, int defaultScale, int minScale, int maxScale, boolean nestedType, String... aliases) {
        this.objectType = toObjectType(javaClass);
        this.widerObjectType = !signed ? toWiderObjectType(javaClass) : this.objectType;
        this.primitiveType = toPrimitiveType(javaClass);
        this.widerPrimitiveType = !signed ? toWiderPrimitiveType(javaClass) : this.primitiveType;
        this.parameter = parameter;
        this.caseSensitive = caseSensitive;
        this.signed = signed;
        this.byteLength = byteLength;
        this.maxPrecision = maxPrecision;
        this.defaultScale = defaultScale;
        this.minScale = minScale;
        this.maxScale = maxScale;
        this.nestedType = nestedType;
        this.binTag = -1;
        if (aliases == null || aliases.length == 0) {
            this.aliases = Collections.emptyList();
        } else {
            this.aliases = Collections.unmodifiableList(Arrays.asList(aliases));
        }
    }

    ClickHouseDataType(Class<?> javaClass, boolean parameter, boolean caseSensitive, boolean signed, int byteLength,
                       int maxPrecision, int defaultScale, int minScale, int maxScale, boolean nestedType, int binTag, String... aliases) {
        this.objectType = toObjectType(javaClass);
        this.widerObjectType = !signed ? toWiderObjectType(javaClass) : this.objectType;
        this.primitiveType = toPrimitiveType(javaClass);
        this.widerPrimitiveType = !signed ? toWiderPrimitiveType(javaClass) : this.primitiveType;
        this.parameter = parameter;
        this.caseSensitive = caseSensitive;
        this.signed = signed;
        this.byteLength = byteLength;
        this.maxPrecision = maxPrecision;
        this.defaultScale = defaultScale;
        this.minScale = minScale;
        this.maxScale = maxScale;
        this.nestedType = nestedType;
        this.binTag = (byte) binTag;
        if (aliases == null || aliases.length == 0) {
            this.aliases = Collections.emptyList();
        } else {
            this.aliases = Collections.unmodifiableList(Arrays.asList(aliases));
        }
    }

    /**
     * Gets Java class for this data type. Prefer wrapper objects to primitives(e.g.
     * {@code Integer.class} instead of {@code int.class}).
     *
     * @return Java class
     */
    public Class<?> getObjectClass() {
        return objectType;
    }

    /**
     * Gets Java class for this data type. Prefer wrapper objects to primitives(e.g.
     * {@code Integer.class} instead of {@code int.class}).
     *
     * @return Java class
     */
    public Class<?> getWiderObjectClass() {
        return widerObjectType;
    }

    /**
     * Gets Java class for this data type. Prefer primitives to wrapper objects(e.g.
     * {@code int.class} instead of {@code Integer.class}).
     *
     * @return Java class
     */
    public Class<?> getPrimitiveClass() {
        return primitiveType;
    }

    /**
     * Gets Java class for this data type. Prefer primitives to wrapper objects(e.g.
     * {@code int.class} instead of {@code Integer.class}).
     *
     * @return Java class
     */
    public Class<?> getWiderPrimitiveClass() {
        return widerPrimitiveType;
    }

    /**
     * Checks if this data type may have parameter(s).
     *
     * @return true if this data type may have parameter; false otherwise
     */
    public boolean hasParameter() {
        return parameter;
    }

    /**
     * Checks if name of this data type is case sensitive or not.
     *
     * @return true if it's case sensitive; false otherwise
     */
    public boolean isCaseSensitive() {
        return caseSensitive;
    }

    /**
     * Checks if this data type uses a nested structure.
     *
     * @return true if it uses a nested structure; false otherwise
     */
    public boolean isNested() {
        return nestedType;
    }

    /**
     * Checks if this data type represents signed number.
     *
     * @return true if it's signed; false otherwise
     */
    public boolean isSigned() {
        return signed;
    }

    /**
     * Gets immutable list of aliases for this data type.
     *
     * @return immutable list of aliases
     */
    public List<String> getAliases() {
        return aliases;
    }

    /**
     * Gets byte length of this data type. Zero means unlimited.
     *
     * @return byte length of this data type
     */
    public int getByteLength() {
        return byteLength;
    }

    /**
     * Gets maximum precision of this data type. Zero means unknown or not
     * supported.
     *
     * @return maximum precision of this data type.
     */
    public int getMaxPrecision() {
        return maxPrecision;
    }

    /**
     * Gets default scale of this data type. Zero means unknown or not supported.
     *
     * @return default scale of this data type.
     */
    public int getDefaultScale() {
        return defaultScale;
    }

    /**
     * Gets minimum scale of this data type. Zero means unknown or not supported.
     *
     * @return minimum scale of this data type.
     */
    public int getMinScale() {
        return minScale;
    }

    /**
     * Gets maximum scale of this data type. Zero means unknown or not supported.
     *
     * @return maximum scale of this data type.
     */
    public int getMaxScale() {
        return maxScale;
    }

    /**
     * Returns a binary tag for the type
     * @return tag value
     */
    public byte getBinTag() {
        return binTag;
    }
}

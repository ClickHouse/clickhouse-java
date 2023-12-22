package com.clickhouse.data;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

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
    Bool(Boolean.class, false, false, true, 1, 1, 0, 0, 0, false, "BOOLEAN"),
    Date(LocalDate.class, false, false, false, 2, 10, 0, 0, 0, false),
    Date32(LocalDate.class, false, false, false, 4, 10, 0, 0, 0, false),
    DateTime(LocalDateTime.class, true, false, false, 0, 29, 0, 0, 9, false, "TIMESTAMP"),
    DateTime32(LocalDateTime.class, true, false, false, 4, 19, 0, 0, 0, false),
    DateTime64(LocalDateTime.class, true, false, false, 8, 29, 3, 0, 9, false),
    Enum8(String.class, true, true, false, 1, 0, 0, 0, 0, false, "ENUM"),
    Enum16(String.class, true, true, false, 2, 0, 0, 0, 0, false),
    FixedString(String.class, true, true, false, 0, 0, 0, 0, 0, false, "BINARY"),
    Int8(Byte.class, false, true, true, 1, 3, 0, 0, 0, false, "BYTE", "INT1", "INT1 SIGNED", "TINYINT",
            "TINYINT SIGNED"),
    UInt8(UnsignedByte.class, false, true, false, 1, 3, 0, 0, 0, false, "INT1 UNSIGNED", "TINYINT UNSIGNED"),
    Int16(Short.class, false, true, true, 2, 5, 0, 0, 0, false, "SMALLINT", "SMALLINT SIGNED"),
    UInt16(UnsignedShort.class, false, true, false, 2, 5, 0, 0, 0, false, "SMALLINT UNSIGNED", "YEAR"),
    Int32(Integer.class, false, true, true, 4, 10, 0, 0, 0, false, "INT", "INT SIGNED", "INTEGER", "INTEGER SIGNED",
            "MEDIUMINT", "MEDIUMINT SIGNED"),
    // https://docs.oracle.com/javase/tutorial/java/nutsandbolts/datatypes.html#PageTitle
    UInt32(UnsignedInteger.class, false, true, false, 4, 10, 0, 0, 0, false, "INT UNSIGNED", "INTEGER UNSIGNED",
            "MEDIUMINT UNSIGNED"),
    Int64(Long.class, false, true, true, 8, 19, 0, 0, 0, false, "BIGINT", "BIGINT SIGNED", "TIME"),
    IntervalYear(Long.class, false, true, true, 8, 19, 0, 0, 0, false),
    IntervalQuarter(Long.class, false, true, true, 8, 19, 0, 0, 0, false),
    IntervalMonth(Long.class, false, true, true, 8, 19, 0, 0, 0, false),
    IntervalWeek(Long.class, false, true, true, 8, 19, 0, 0, 0, false),
    IntervalDay(Long.class, false, true, true, 8, 19, 0, 0, 0, false),
    IntervalHour(Long.class, false, true, true, 8, 19, 0, 0, 0, false),
    IntervalMinute(Long.class, false, true, true, 8, 19, 0, 0, 0, false),
    IntervalSecond(Long.class, false, true, true, 8, 19, 0, 0, 0, false),
    IntervalMicrosecond(Long.class, false, true, true, 8, 19, 0, 0, 0, false),
    IntervalMillisecond(Long.class, false, true, true, 8, 19, 0, 0, 0, false),
    IntervalNanosecond(Long.class, false, true, true, 8, 19, 0, 0, 0, false),
    UInt64(UnsignedLong.class, false, true, false, 8, 20, 0, 0, 0, false, "BIGINT UNSIGNED", "BIT", "SET"),
    Int128(BigInteger.class, false, true, true, 16, 39, 0, 0, 0, false),
    UInt128(BigInteger.class, false, true, false, 16, 39, 0, 0, 0, false),
    Int256(BigInteger.class, false, true, true, 32, 77, 0, 0, 0, false),
    UInt256(BigInteger.class, false, true, false, 32, 78, 0, 0, 0, false),
    Decimal(BigDecimal.class, true, false, true, 0, 76, 0, 0, 76, false, "DEC", "FIXED", "NUMERIC"),
    Decimal32(BigDecimal.class, true, false, true, 4, 9, 9, 0, 9, false),
    Decimal64(BigDecimal.class, true, false, true, 8, 18, 18, 0, 18, false),
    Decimal128(BigDecimal.class, true, false, true, 16, 38, 38, 0, 38, false),
    Decimal256(BigDecimal.class, true, false, true, 32, 76, 20, 0, 76, false),
    Float32(Float.class, false, true, true, 4, 12, 0, 0, 38, false, "FLOAT", "REAL", "SINGLE"),
    Float64(Double.class, false, true, true, 8, 22, 0, 0, 308, false, "DOUBLE", "DOUBLE PRECISION"),
    IPv4(Inet4Address.class, false, true, false, 4, 10, 0, 0, 0, false, "INET4"),
    IPv6(Inet6Address.class, false, true, false, 16, 39, 0, 0, 0, false, "INET6"),
    UUID(UUID.class, false, true, false, 16, 69, 0, 0, 0, false),
    Point(Object.class, false, true, true, 33, 0, 0, 0, 0, true), // same as Tuple(Float64, Float64)
    Polygon(Object.class, false, true, true, 0, 0, 0, 0, 0, true), // same as Array(Ring)
    MultiPolygon(Object.class, false, true, true, 0, 0, 0, 0, 0, true), // same as Array(Polygon)
    Ring(Object.class, false, true, true, 0, 0, 0, 0, 0, true), // same as Array(Point)
    JSON(Object.class, false, false, false, 0, 0, 0, 0, 0, true), // same as Object('JSON')
    Object(Object.class, true, true, false, 0, 0, 0, 0, 0, true),
    String(String.class, false, false, false, 0, 0, 0, 0, 0, false, "BINARY LARGE OBJECT", "BINARY VARYING", "BLOB",
            "BYTEA", "CHAR", "CHAR LARGE OBJECT", "CHAR VARYING", "CHARACTER", "CHARACTER LARGE OBJECT",
            "CHARACTER VARYING", "CLOB", "GEOMETRY", "LONGBLOB", "LONGTEXT", "MEDIUMBLOB", "MEDIUMTEXT",
            "NATIONAL CHAR", "NATIONAL CHAR VARYING", "NATIONAL CHARACTER", "NATIONAL CHARACTER LARGE OBJECT",
            "NATIONAL CHARACTER VARYING", "NCHAR", "NCHAR LARGE OBJECT", "NCHAR VARYING", "NVARCHAR", "TEXT",
            "TINYBLOB", "TINYTEXT", "VARBINARY", "VARCHAR", "VARCHAR2"),
    Array(Object.class, true, true, false, 0, 0, 0, 0, 0, true),
    Map(Map.class, true, true, false, 0, 0, 0, 0, 0, true),
    Nested(Object.class, true, true, false, 0, 0, 0, 0, 0, true),
    Tuple(List.class, true, true, false, 0, 0, 0, 0, 0, true),
    Nothing(Object.class, false, true, false, 0, 0, 0, 0, 0, true),
    SimpleAggregateFunction(String.class, true, true, false, 0, 0, 0, 0, 0, false),
    // implementation-defined intermediate state
    AggregateFunction(String.class, true, true, false, 0, 0, 0, 0, 0, true);

    /**
     * Immutable set(sorted) for all aliases.
     */
    public static final Set<String> allAliases;

    /**
     * Immutable mapping between name and type.
     */
    public static final Map<String, ClickHouseDataType> name2type;

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
    }

    /**
     * Checks if the given type name is an alias or not.
     *
     * @param typeName type name
     * @return true if the type name is an alias; false otherwise
     */
    public static boolean isAlias(String typeName) {
        return typeName != null && !typeName.isEmpty() && allAliases.contains(typeName.trim().toUpperCase());
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
}

package com.clickhouse.client;

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
    IntervalYear(Long.class, false, true, true, 8, 19, 0, 0, 0, false, false),
    IntervalQuarter(Long.class, false, true, true, 8, 19, 0, 0, 0, false, false),
    IntervalMonth(Long.class, false, true, true, 8, 19, 0, 0, 0, false, false),
    IntervalWeek(Long.class, false, true, true, 8, 19, 0, 0, 0, false, false),
    IntervalDay(Long.class, false, true, true, 8, 19, 0, 0, 0, false, false),
    IntervalHour(Long.class, false, true, true, 8, 19, 0, 0, 0, false, false),
    IntervalMinute(Long.class, false, true, true, 8, 19, 0, 0, 0, false, false),
    IntervalSecond(Long.class, false, true, true, 8, 19, 0, 0, 0, false, false),
    UInt8(Short.class, false, true, false, 1, 3, 0, 0, 0, false, true, "INT1 UNSIGNED", "TINYINT UNSIGNED"),
    UInt16(Integer.class, false, true, false, 2, 5, 0, 0, 0, false, true, "SMALLINT UNSIGNED"),
    UInt32(Long.class, false, true, false, 4, 10, 0, 0, 0, false, true, "INT UNSIGNED", "INTEGER UNSIGNED",
            "MEDIUMINT UNSIGNED"),
    UInt64(Long.class, false, true, false, 8, 20, 0, 0, 0, false, true, "BIGINT UNSIGNED"),
    UInt128(BigInteger.class, false, true, false, 16, 39, 0, 0, 0, false, true),
    UInt256(BigInteger.class, false, true, false, 32, 78, 0, 0, 0, false, true),
    Int8(Byte.class, false, true, true, 1, 3, 0, 0, 0, false, true, "BYTE", "INT1", "INT1 SIGNED", "TINYINT",
            "TINYINT SIGNED"),
    Int16(Short.class, false, true, true, 2, 5, 0, 0, 0, false, true, "SMALLINT", "SMALLINT SIGNED"),
    Int32(Integer.class, false, true, true, 4, 10, 0, 0, 0, false, true, "INT", "INTEGER", "MEDIUMINT", "INT SIGNED",
            "INTEGER SIGNED", "MEDIUMINT SIGNED"),
    Int64(Long.class, false, true, true, 8, 19, 0, 0, 0, false, true, "BIGINT", "BIGINT SIGNED"),
    Int128(BigInteger.class, false, true, true, 16, 39, 0, 0, 0, false, true),
    Int256(BigInteger.class, false, true, true, 32, 77, 0, 0, 0, false, true),
    Bool(Boolean.class, false, false, true, 1, 1, 0, 0, 0, false, true, "BOOLEAN"),
    Date(LocalDate.class, false, false, false, 2, 10, 0, 0, 0, false, true),
    Date32(LocalDate.class, false, false, false, 4, 10, 0, 0, 0, false, true),
    DateTime(LocalDateTime.class, true, false, false, 0, 29, 0, 0, 9, false, true, "TIMESTAMP"),
    DateTime32(LocalDateTime.class, true, false, false, 4, 19, 0, 0, 0, false, true),
    DateTime64(LocalDateTime.class, true, false, false, 8, 29, 3, 0, 9, false, true),
    Decimal(BigDecimal.class, true, false, true, 0, 76, 0, 0, 76, false, true, "DEC", "NUMERIC", "FIXED"),
    Decimal32(BigDecimal.class, true, false, true, 4, 9, 9, 0, 9, false, true),
    Decimal64(BigDecimal.class, true, false, true, 8, 18, 18, 0, 18, false, true),
    Decimal128(BigDecimal.class, true, false, true, 16, 38, 38, 0, 38, false, true),
    Decimal256(BigDecimal.class, true, false, true, 32, 76, 20, 0, 76, false, true),
    UUID(UUID.class, false, true, false, 16, 69, 0, 0, 0, false, false),
    /**
     * Enum data type.
     *
     * @deprecated will be removed in v0.3.3, please use {@link #Enum8} instead
     */
    @Deprecated
    Enum(String.class, true, true, false, 1, 0, 0, 0, 0, false, true),
    Enum8(String.class, true, true, false, 1, 0, 0, 0, 0, false, true), // "ENUM"),
    Enum16(String.class, true, true, false, 2, 0, 0, 0, 0, false, true),
    Float32(Float.class, false, true, true, 4, 12, 0, 0, 38, false, true, "FLOAT", "REAL", "SINGLE"),
    Float64(Double.class, false, true, true, 16, 22, 0, 0, 308, false, true, "DOUBLE", "DOUBLE PRECISION"),
    IPv4(Inet4Address.class, false, true, false, 4, 10, 0, 0, 0, false, false, "INET4"),
    IPv6(Inet6Address.class, false, true, false, 16, 39, 0, 0, 0, false, false, "INET6"),
    FixedString(String.class, true, true, false, 0, 0, 0, 0, 0, false, false, "BINARY"),
    String(String.class, false, true, false, 0, 0, 0, 0, 0, false, true, "BINARY LARGE OBJECT", "BINARY VARYING", "BLOB",
            "BYTEA",
            "CHAR", "CHARACTER", "CHARACTER LARGE OBJECT", "CHARACTER VARYING", "CHAR LARGE OBJECT", "CHAR VARYING",
            "CLOB", "LONGBLOB", "LONGTEXT", "MEDIUMBLOB", "MEDIUMTEXT", "NATIONAL CHAR", "NATIONAL CHARACTER",
            "NATIONAL CHARACTER LARGE OBJECT", "NATIONAL CHARACTER VARYING", "NATIONAL CHAR VARYING", "NCHAR",
            "NCHAR LARGE OBJECT", "NCHAR VARYING", "NVARCHAR", "TEXT", "TINYBLOB", "TINYTEXT", "VARBINARY", "VARCHAR",
            "VARCHAR2"),
    AggregateFunction(String.class, true, true, false, 0, 0, 0, 0, 0, true, false), // implementation-defined intermediate
    // state
    SimpleAggregateFunction(String.class, true, true, false, 0, 0, 0, 0, 0, false, false),
    Array(Object.class, true, true, false, 0, 0, 0, 0, 0, true, false),
    Map(Map.class, true, true, false, 0, 0, 0, 0, 0, true, false),
    Nested(Object.class, true, true, false, 0, 0, 0, 0, 0, true, false),
    Tuple(List.class, true, true, false, 0, 0, 0, 0, 0, true, false),
    Object(Object.class, true, true, false, 0, 0, 0, 0, 0, true, false),
    JSON(Object.class, false, false, false, 0, 0, 0, 0, 0, true, true), // same as Object('JSON')
    Point(Object.class, false, true, true, 33, 0, 0, 0, 0, true, false), // same as Tuple(Float64, Float64)
    Polygon(Object.class, false, true, true, 0, 0, 0, 0, 0, true, false), // same as Array(Ring)
    MultiPolygon(Object.class, false, true, true, 0, 0, 0, 0, 0, true, false), // same as Array(Polygon)
    Ring(Object.class, false, true, true, 0, 0, 0, 0, 0, true, false), // same as Array(Point)
    Nothing(Object.class, false, true, false, 0, 0, 0, 0, 0, true, false);

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
            if (t.isCaseInsensitiveTypeName()) {
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
                if (alias.length() == part.length() && alias.equals(part)) {
                    types.add(alias);
                } else if (alias.length() > part.length() && alias.startsWith(prefix)) {
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
        if (typeName == null || (typeName = typeName.trim()).isEmpty()) {
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
        if (byte.class == javaClass || boolean.class == javaClass || Boolean.class == javaClass) {
            javaClass = Byte.class;
        } else if (short.class == javaClass) {
            javaClass = Short.class;
        } else if (int.class == javaClass || char.class == javaClass || Character.class == javaClass) {
            javaClass = Integer.class;
        } else if (long.class == javaClass) {
            javaClass = Long.class;
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
        if (Byte.class == javaClass || Boolean.class == javaClass || boolean.class == javaClass) {
            javaClass = byte.class;
        } else if (Short.class == javaClass) {
            javaClass = short.class;
        } else if (Integer.class == javaClass || Character.class == javaClass || char.class == javaClass) {
            javaClass = int.class;
        } else if (Long.class == javaClass) {
            javaClass = long.class;
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
    private final Class<?> primitiveType;
    private final boolean parameter;
    private final boolean caseSensitive;
    private final boolean caseInsensitiveTypeName;
    private final boolean signed;
    private final List<String> aliases;
    private final int byteLength;
    private final int maxPrecision;
    private final int defaultScale;
    private final int minScale;
    private final int maxScale;
    private final boolean nestedType;

    ClickHouseDataType(Class<?> javaClass, boolean parameter, boolean caseSensitive, boolean signed, int byteLength,
                       int maxPrecision, int defaultScale, int minScale, int maxScale, boolean nestedType, boolean caseInsensitiveTypeName, String... aliases) {
        this.objectType = toObjectType(javaClass);
        this.primitiveType = toPrimitiveType(javaClass);
        this.parameter = parameter;
        this.caseSensitive = caseSensitive;
        this.signed = signed;
        this.byteLength = byteLength;
        this.maxPrecision = maxPrecision;
        this.defaultScale = defaultScale;
        this.minScale = minScale;
        this.maxScale = maxScale;
        this.nestedType = nestedType;
        this.caseInsensitiveTypeName = caseInsensitiveTypeName;
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
     * Gets Java class for this data type. Prefer primitives to wrapper objects(e.g.
     * {@code int.class} instead of {@code Integer.class}).
     *
     * @return Java class
     */
    public Class<?> getPrimitiveClass() {
        return primitiveType;
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
     * Checks if name of this data type is case insensitive or not when look up type with type name
     *
     * @return true if it's case insensitive; false otherwise
     */
    public boolean isCaseInsensitiveTypeName() {
        return caseInsensitiveTypeName;
    }

}

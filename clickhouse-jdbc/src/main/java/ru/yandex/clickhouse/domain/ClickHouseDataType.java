package ru.yandex.clickhouse.domain;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Array;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

/**
 * Basic ClickHouse data types.
 * <p>
 * This list is based on the list of data type families returned by
 * {@code SELECT * FROM system.data_type_families}
 * <p>
 * {@code LowCardinality} and {@code Nullable} are technically data types in
 * ClickHouse, but for the sake of this driver, we treat these data types as
 * modifiers for the underlying base data types.
 */
public enum ClickHouseDataType {
    // aliases:
    // https://clickhouse.tech/docs/en/sql-reference/data-types/multiword-types/
    // https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/DataTypeCustomIPv4AndIPv6.cpp
    // https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/registerDataTypeDateTime.cpp
    // https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/DataTypesDecimal.cpp
    // https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/DataTypeFixedString.cpp
    // https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/DataTypesNumber.cpp
    // https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/DataTypeString.cpp
    IntervalYear(JDBCType.INTEGER, Integer.class, true, 19, 0),
    IntervalQuarter(JDBCType.INTEGER, Integer.class, true, 19, 0),
    IntervalMonth(JDBCType.INTEGER, Integer.class, true, 19, 0),
    IntervalWeek(JDBCType.INTEGER, Integer.class, true, 19, 0),
    IntervalDay(JDBCType.INTEGER, Integer.class, true, 19, 0),
    IntervalHour(JDBCType.INTEGER, Integer.class, true, 19, 0),
    IntervalMinute(JDBCType.INTEGER, Integer.class, true, 19, 0),
    IntervalSecond(JDBCType.INTEGER, Integer.class, true, 19, 0),
    UInt256(JDBCType.NUMERIC, BigInteger.class, true, 39, 0),
    UInt128(JDBCType.NUMERIC, BigInteger.class, true, 20, 0),
    UInt64(JDBCType.BIGINT, BigInteger.class, false, 19, 0,
            "BIGINT UNSIGNED"),
    UInt32(JDBCType.BIGINT, Long.class, false, 10, 0,
            "INT UNSIGNED", "INTEGER UNSIGNED", "MEDIUMINT UNSIGNED"),
    UInt16(JDBCType.SMALLINT, Integer.class, false, 5, 0,
            "SMALLINT UNSIGNED"),
    UInt8(JDBCType.TINYINT, Integer.class, false, 3, 0,
            "TINYINT UNSIGNED", "INT1 UNSIGNED"),
    Int256(JDBCType.NUMERIC, BigInteger.class, true, 40, 0),
    Int128(JDBCType.NUMERIC, BigInteger.class, true, 20, 0),
    Int64(JDBCType.BIGINT, Long.class, true, 20, 0,
            "BIGINT", "BIGINT SIGNED"),
    Int32(JDBCType.INTEGER, Integer.class, true, 11, 0,
            "INT", "INTEGER", "MEDIUMINT", "INT SIGNED", "INTEGER SIGNED", "MEDIUMINT SIGNED"),
    Int16(JDBCType.SMALLINT, Integer.class, true, 6, 0,
            "SMALLINT", "SMALLINT SIGNED"),
    Int8(JDBCType.TINYINT, Integer.class, true, 4, 0,
            "TINYINT", "BOOL", "BOOLEAN", "INT1", "BYTE", "TINYINT SIGNED", "INT1 SIGNED"),
    Date(JDBCType.DATE, Date.class, false, 10, 0),
    DateTime(JDBCType.TIMESTAMP, Timestamp.class, false, 19, 0,
            "TIMESTAMP"),
    DateTime32(JDBCType.TIMESTAMP, Timestamp.class, false, 19, 0),
    DateTime64(JDBCType.TIMESTAMP, Timestamp.class, false, 38, 3), // scale up to 18
    Enum8(JDBCType.VARCHAR, String.class, false, 0, 0,
            "ENUM"),
    Enum16(JDBCType.VARCHAR, String.class, false, 0, 0),
    Float32(JDBCType.REAL, Float.class, true, 8, 8,
            "SINGLE", "REAL"),
    Float64(JDBCType.DOUBLE, Double.class, true, 17, 17,
            "DOUBLE", "DOUBLE PRECISION"),
    Decimal32(JDBCType.DECIMAL, BigDecimal.class, true, 9, 9),
    Decimal64(JDBCType.DECIMAL, BigDecimal.class, true, 18, 18),
    Decimal128(JDBCType.DECIMAL, BigDecimal.class, true, 38, 38),
    Decimal256(JDBCType.DECIMAL, BigDecimal.class, true, 76, 20),
    Decimal(JDBCType.DECIMAL, BigDecimal.class, true, 0, 0,
            "DEC", "NUMERIC", "FIXED"),
    UUID(JDBCType.OTHER, UUID.class, false, 36, 0),
    IPv4(JDBCType.VARCHAR, String.class, false, 10, 0),
    IPv6(JDBCType.VARCHAR, String.class, false, 0, 0),
    String(JDBCType.VARCHAR, String.class, false, 0, 0,
            "CHAR", "NCHAR", "CHARACTER", "VARCHAR", "NVARCHAR", "VARCHAR2",
            "TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT",
            "BLOB", "CLOB", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB", "BYTEA",
            "CHARACTER LARGE OBJECT", "CHARACTER VARYING", "CHAR LARGE OBJECT", "CHAR VARYING",
            "NATIONAL CHAR", "NATIONAL CHARACTER", "NATIONAL CHARACTER LARGE OBJECT",
            "NATIONAL CHARACTER VARYING", "NATIONAL CHAR VARYING",
            "NCHAR VARYING", "NCHAR LARGE OBJECT", "BINARY LARGE OBJECT", "BINARY VARYING"),
    FixedString(JDBCType.CHAR, String.class, false, -1, 0,
            "BINARY"),
    Nothing(JDBCType.NULL, Object.class, false, 0, 0),
    Nested(JDBCType.STRUCT, String.class, false, 0, 0),
    // TODO use list/collection for Tuple
    Tuple(JDBCType.OTHER, String.class, false, 0, 0),
    Array(JDBCType.ARRAY, Array.class, false, 0, 0),
    Map(JDBCType.OTHER, Map.class, false, 0, 0),
    AggregateFunction(JDBCType.OTHER, String.class, false, 0, 0),
    Unknown(JDBCType.OTHER, String.class, false, 0, 0);

    private static final Map<String, ClickHouseDataType> name2type;

    static {
        Map<String, ClickHouseDataType> map = new HashMap<>();
        String errorMsg = "[%s] is used by type [%s]";
        ClickHouseDataType used = null;
        for (ClickHouseDataType t : ClickHouseDataType.values()) {
            used = map.put(t.name(), t);
            if (used != null) {
                throw new IllegalStateException(java.lang.String.format(errorMsg, t.name(), used.name()));
            }
            String nameInUpperCase = t.name().toUpperCase(Locale.ROOT);
            if (!nameInUpperCase.equals(t.name())) {
                used = map.put(nameInUpperCase, t);
                if (used != null) {
                    throw new IllegalStateException(java.lang.String.format(errorMsg, nameInUpperCase, used.name()));
                }
            }
            for (String alias : t.aliases) {
                used = map.put(alias.toUpperCase(Locale.ROOT), t);
                if (used != null) {
                    throw new IllegalStateException(java.lang.String.format(errorMsg, alias, used.name()));
                }
            }
        }
        name2type = Collections.unmodifiableMap(map);
    }

    private final JDBCType jdbcType;
    private final Class<?> javaClass;
    private final boolean signed;
    private final int defaultPrecision;
    private final int defaultScale;
    private final String[] aliases;

    ClickHouseDataType(JDBCType jdbcType, Class<?> javaClass,
            boolean signed, int defaultPrecision, int defaultScale,
            String... aliases) {
        this.jdbcType = jdbcType;
        this.javaClass = javaClass;
        this.signed = signed;
        this.defaultPrecision = defaultPrecision;
        this.defaultScale = defaultScale;
        this.aliases = aliases;
    }

    public int getSqlType() {
        return jdbcType.getVendorTypeNumber().intValue();
    }

    public JDBCType getJdbcType() {
        return jdbcType;
    }

    public Class<?> getJavaClass() {
        return javaClass;
    }

    public boolean isSigned() {
        return signed;
    }

    public int getDefaultPrecision() {
        return defaultPrecision;
    }

    public int getDefaultScale() {
        return defaultScale;
    }

    public static ClickHouseDataType fromTypeString(String typeString) {
        return name2type.getOrDefault(typeString.trim().toUpperCase(Locale.ROOT), ClickHouseDataType.Unknown);
    }

    public static ClickHouseDataType resolveDefaultArrayDataType(String typeName) {
        return name2type.getOrDefault(typeName, ClickHouseDataType.String);
    }
}

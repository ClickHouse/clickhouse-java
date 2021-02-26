package ru.yandex.clickhouse.domain;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Array;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.Timestamp;
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

    IntervalYear      (JDBCType.INTEGER,   Integer.class,    true,  19,  0),
    IntervalQuarter   (JDBCType.INTEGER,   Integer.class,    true,  19,  0),
    IntervalMonth     (JDBCType.INTEGER,   Integer.class,    true,  19,  0),
    IntervalWeek      (JDBCType.INTEGER,   Integer.class,    true,  19,  0),
    IntervalDay       (JDBCType.INTEGER,   Integer.class,    true,  19,  0),
    IntervalHour      (JDBCType.INTEGER,   Integer.class,    true,  19,  0),
    IntervalMinute    (JDBCType.INTEGER,   Integer.class,    true,  19,  0),
    IntervalSecond    (JDBCType.INTEGER,   Integer.class,    true,  19,  0),
    UInt64            (JDBCType.BIGINT,    BigInteger.class, false, 19,  0),
    UInt32            (JDBCType.BIGINT,    Long.class,       false, 10,  0),
    UInt16            (JDBCType.SMALLINT,  Integer.class,    false,  5,  0),
    UInt8             (JDBCType.TINYINT,   Integer.class,    false,  3,  0),
    Int64             (JDBCType.BIGINT,    Long.class,       true,  20,  0,
        "BIGINT"),
    Int32             (JDBCType.INTEGER,   Integer.class,    true,  11,  0,
        "INTEGER",
        "INT"),
    Int16             (JDBCType.SMALLINT,  Integer.class,    true,   6,  0,
        "SMALLINT"),
    Int8              (JDBCType.TINYINT,   Integer.class,    true,   4,  0,
        "TINYINT"),
    Date              (JDBCType.DATE,      Date.class,       false, 10,  0),
    DateTime          (JDBCType.TIMESTAMP, Timestamp.class,  false, 19,  0,
        "TIMESTAMP"),
    Enum8             (JDBCType.VARCHAR,   String.class,     false,  0,  0),
    Enum16            (JDBCType.VARCHAR,   String.class,     false,  0,  0),
    Float32           (JDBCType.REAL,      Float.class,      true,   8,  8,
        "REAL"),
    Float64           (JDBCType.DOUBLE,    Double.class,     true,  17, 17,
        "DOUBLE"),
    Decimal32         (JDBCType.DECIMAL,   BigDecimal.class, true,   9,  9),
    Decimal64         (JDBCType.DECIMAL,   BigDecimal.class, true,  18, 18),
    Decimal128        (JDBCType.DECIMAL,   BigDecimal.class, true,  38, 38),
    Decimal           (JDBCType.DECIMAL,   BigDecimal.class, true,   0,  0,
        "DEC"),
    UUID              (JDBCType.OTHER,     UUID.class,       false, 36,  0),
    String            (JDBCType.VARCHAR,   String.class,     false,  0,  0,
        "LONGBLOB",
        "MEDIUMBLOB",
        "TINYBLOB",
        "MEDIUMTEXT",
        "CHAR",
        "VARCHAR",
        "TEXT",
        "TINYTEXT",
        "LONGTEXT",
        "BLOB"),
    FixedString       (JDBCType.CHAR,      String.class,     false, -1,  0,
        "BINARY"),
    Nothing           (JDBCType.NULL,      Object.class,     false,  0,  0),
    Nested            (JDBCType.STRUCT,    String.class,     false,  0,  0),
    Tuple             (JDBCType.OTHER,     String.class,     false,  0,  0),
    Array             (JDBCType.ARRAY,     Array.class,      false,  0,  0),
    AggregateFunction (JDBCType.OTHER,     String.class,     false,  0,  0),
    Unknown           (JDBCType.OTHER,     String.class,     false,  0,  0);

    private final JDBCType jdbcType;
    private final Class<?> javaClass;
    private final boolean signed;
    private final int defaultPrecision;
    private final int defaultScale;
    private final String[] aliases;

    ClickHouseDataType(JDBCType jdbcType, Class<?> javaClass,
        boolean signed, int defaultPrecision, int defaultScale,
        String... aliases)
    {
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
        String s = typeString.trim();
        for (ClickHouseDataType dataType : values()) {
            if (s.equalsIgnoreCase(dataType.name()))            {
                return dataType;
            }
            for (String alias : dataType.aliases) {
                if (s.equalsIgnoreCase(alias)) {
                    return dataType;
                }
            }
        }
        return ClickHouseDataType.Unknown;
    }

    public static ClickHouseDataType resolveDefaultArrayDataType(String typeName) {
        for (ClickHouseDataType chDataType : values()) {
            if (chDataType.name().equals(typeName)) {
                return chDataType;
            }
        }
        return ClickHouseDataType.String;
    }

}

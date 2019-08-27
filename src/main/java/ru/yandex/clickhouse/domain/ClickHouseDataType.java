package ru.yandex.clickhouse.domain;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Array;
import java.sql.Date;
import java.sql.Timestamp;
import java.sql.Types;
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

    IntervalYear      (Types.INTEGER,   Integer.class,    true,  19,  0),
    IntervalQuarter   (Types.INTEGER,   Integer.class,    true,  19,  0),
    IntervalMonth     (Types.INTEGER,   Integer.class,    true,  19,  0),
    IntervalWeek      (Types.INTEGER,   Integer.class,    true,  19,  0),
    IntervalDay       (Types.INTEGER,   Integer.class,    true,  19,  0),
    IntervalHour      (Types.INTEGER,   Integer.class,    true,  19,  0),
    IntervalMinute    (Types.INTEGER,   Integer.class,    true,  19,  0),
    IntervalSecond    (Types.INTEGER,   Integer.class,    true,  19,  0),
    UInt64            (Types.BIGINT,    BigInteger.class, false, 19,  0),
    UInt32            (Types.INTEGER,   Long.class,       false, 10,  0),
    UInt16            (Types.SMALLINT,  Integer.class,    false,  5,  0),
    UInt8             (Types.TINYINT,   Integer.class,    false,  3,  0),
    Int64             (Types.BIGINT,    Long.class,       true,  20,  0,
        "BIGINT"),
    Int32             (Types.INTEGER,   Integer.class,    true,  11,  0,
        "INTEGER",
        "INT"),
    Int16             (Types.SMALLINT,  Integer.class,    true,   6,  0,
        "SMALLINT"),
    Int8              (Types.TINYINT,   Integer.class,    true,   4,  0,
        "TINYINT"),
    Date              (Types.DATE,      Date.class,       false, 10,  0),
    DateTime          (Types.TIMESTAMP, Timestamp.class,  false, 19,  0,
        "TIMESTAMP"),
    Enum8             (Types.VARCHAR,   String.class,     false,  0,  0),
    Enum16            (Types.VARCHAR,   String.class,     false,  0,  0),
    Float32           (Types.FLOAT,     Float.class,      true,   8,  8,
        "FLOAT"),
    Float64           (Types.DOUBLE,    Double.class,     true,  17, 17,
        "DOUBLE"),
    Decimal32         (Types.DECIMAL,   BigDecimal.class, true,   9,  9),
    Decimal64         (Types.DECIMAL,   BigDecimal.class, true,  18, 18),
    Decimal128        (Types.DECIMAL,   BigDecimal.class, true,  38, 38),
    Decimal           (Types.DECIMAL,   BigDecimal.class, true,   0,  0,
        "DEC"),
    UUID              (Types.OTHER,     UUID.class,       false, 36,  0),
    String            (Types.VARCHAR,   String.class,     false,  0,  0,
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
    FixedString       (Types.CHAR,      String.class,     false, -1,  0,
        "BINARY"),
    Nothing           (Types.NULL,      Object.class,     false,  0,  0),
    Nested            (Types.STRUCT,    String.class,     false,  0,  0),
    Tuple             (Types.OTHER,     String.class,     false,  0,  0),
    Array             (Types.ARRAY,     Array.class,      false,  0,  0),
    AggregateFunction (Types.OTHER,     String.class,     false,  0,  0),
    Unknown           (Types.OTHER,     String.class,     false,  0,  0);

    private final int sqlType;
    private final Class<?> javaClass;
    private final boolean signed;
    private final int defaultPrecision;
    private final int defaultScale;
    private final String[] aliases;

    ClickHouseDataType(int sqlType, Class<?> javaClass,
        boolean signed, int defaultPrecision, int defaultScale,
        String... aliases)
    {
        this.sqlType = sqlType;
        this.javaClass = javaClass;
        this.signed = signed;
        this.defaultPrecision = defaultPrecision;
        this.defaultScale = defaultScale;
        this.aliases = aliases;
    }

    public int getSqlType() {
        return sqlType;
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

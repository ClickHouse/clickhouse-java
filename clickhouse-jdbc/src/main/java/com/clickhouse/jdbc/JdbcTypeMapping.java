package com.clickhouse.jdbc;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseUtils;

/**
 * This class defines mappings among {@link Types}, {@link JDBCType},
 * {@link ClickHouseDataType}, {@link ClickHouseColumn}, and {@link Class}. It
 * does not impact serialization and deserialization, which is handled
 * separately by {@link com.clickhouse.data.ClickHouseDataProcessor}.
 */
public class JdbcTypeMapping {
    /**
     * Gets corresponding {@link ClickHouseDataType} of the given {@link Types}.
     *
     * @param sqlType generic SQL types defined in JDBC
     * @return non-null ClickHouse data type
     */
    protected static ClickHouseDataType getDataType(int sqlType) {
        ClickHouseDataType dataType;

        switch (sqlType) {
            case Types.BOOLEAN:
                dataType = ClickHouseDataType.UInt8;
                break;
            case Types.TINYINT:
                dataType = ClickHouseDataType.Int8;
                break;
            case Types.SMALLINT:
                dataType = ClickHouseDataType.Int16;
                break;
            case Types.INTEGER:
                dataType = ClickHouseDataType.Int32;
                break;
            case Types.BIGINT:
                dataType = ClickHouseDataType.Int64;
                break;
            case Types.NUMERIC:
                dataType = ClickHouseDataType.Int256;
                break;
            case Types.FLOAT:
            case Types.REAL:
                dataType = ClickHouseDataType.Float32;
                break;
            case Types.DOUBLE:
                dataType = ClickHouseDataType.Float64;
                break;
            case Types.DECIMAL:
                dataType = ClickHouseDataType.Decimal;
                break;
            case Types.BIT:
            case Types.BLOB:
            case Types.BINARY:
            case Types.CHAR:
            case Types.CLOB:
            case Types.JAVA_OBJECT:
            case Types.LONGNVARCHAR:
            case Types.LONGVARBINARY:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NCLOB:
            case Types.NVARCHAR:
            case Types.OTHER:
            case Types.SQLXML:
            case Types.VARBINARY:
            case Types.VARCHAR:
                dataType = ClickHouseDataType.String;
                break;
            case Types.DATE:
                dataType = ClickHouseDataType.Date;
                break;
            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE:
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                dataType = ClickHouseDataType.DateTime;
                break;
            case Types.ARRAY:
                dataType = ClickHouseDataType.Array;
                break;
            case Types.STRUCT:
                dataType = ClickHouseDataType.Tuple;
                break;
            case Types.DATALINK:
            case Types.DISTINCT:
            case Types.REF:
            case Types.REF_CURSOR:
            case Types.ROWID:
            case Types.NULL:
            default:
                dataType = ClickHouseDataType.Nothing;
                break;
        }
        return dataType;
    }

    /**
     * Gets corresponding {@link Types} for the given Java class.
     *
     * @param javaClass non-null Java class
     * @return generic SQL type defined in JDBC
     */
    protected static int getSqlType(Class<?> javaClass) { // and purpose(e.g. for read or write?)
        final int sqlType;
        if (javaClass == boolean.class || javaClass == Boolean.class) {
            sqlType = Types.BOOLEAN;
        } else if (javaClass == byte.class || javaClass == Byte.class) {
            sqlType = Types.TINYINT;
        } else if (javaClass == short.class || javaClass == Short.class) {
            sqlType = Types.SMALLINT;
        } else if (javaClass == int.class || javaClass == Integer.class) {
            sqlType = Types.INTEGER;
        } else if (javaClass == long.class || javaClass == Long.class) {
            sqlType = Types.BIGINT;
        } else if (javaClass == float.class || javaClass == Float.class) {
            sqlType = Types.FLOAT;
        } else if (javaClass == double.class || javaClass == Double.class) {
            sqlType = Types.DOUBLE;
        } else if (javaClass == BigInteger.class) {
            sqlType = Types.NUMERIC;
        } else if (javaClass == BigDecimal.class) {
            sqlType = Types.DECIMAL;
        } else if (javaClass == Date.class || javaClass == LocalDate.class) {
            sqlType = Types.DATE;
        } else if (javaClass == Time.class || javaClass == LocalTime.class) {
            sqlType = Types.TIME;
        } else if (javaClass == Timestamp.class || javaClass == LocalDateTime.class) {
            sqlType = Types.TIMESTAMP;
        } else if (javaClass == OffsetDateTime.class || javaClass == ZonedDateTime.class) {
            sqlType = Types.TIMESTAMP_WITH_TIMEZONE;
        } else if (javaClass == String.class || javaClass == byte[].class || Enum.class.isAssignableFrom(javaClass)) {
            sqlType = Types.VARCHAR;
        } else if (javaClass.isArray()) { // could be Nested type
            sqlType = Types.ARRAY;
        } else if (List.class.isAssignableFrom(javaClass) || Map.class.isAssignableFrom(javaClass)) {
            sqlType = Types.STRUCT;
        } else {
            sqlType = Types.OTHER;
        }
        return sqlType;
    }

    /**
     * Converts {@link JDBCType} to ClickHouse column.
     *
     * @param jdbcType      JDBC type
     * @param scaleOrLength scale or length
     * @return non-null ClickHouse column
     */
    public static ClickHouseColumn toColumn(JDBCType jdbcType, int scaleOrLength) {
        Integer type = jdbcType.getVendorTypeNumber();
        return toColumn(type != null ? type : Types.OTHER, scaleOrLength);
    }

    /**
     * Converts {@link Types} to ClickHouse column.
     *
     * @param sqlType       generic SQL types defined in JDBC
     * @param scaleOrLength scale or length
     * @return non-null ClickHouse column
     */
    public static ClickHouseColumn toColumn(int sqlType, int scaleOrLength) {
        ClickHouseDataType dataType = getDataType(sqlType);
        ClickHouseColumn column = null;
        if (scaleOrLength > 0) {
            if (sqlType == Types.BIT && scaleOrLength == 1) {
                dataType = ClickHouseDataType.UInt8;
            } else if (sqlType == Types.NUMERIC || sqlType == Types.DECIMAL) {
                for (ClickHouseDataType t : new ClickHouseDataType[] {}) {
                    if (scaleOrLength <= t.getMaxScale() / 2) {
                        column = ClickHouseColumn.of("", t, false, t.getMaxPrecision() - t.getMaxScale(),
                                scaleOrLength);
                        break;
                    }
                }
            } else if (dataType == ClickHouseDataType.Date) {
                if (scaleOrLength > 2) {
                    dataType = ClickHouseDataType.Date32;
                }
            } else if (dataType == ClickHouseDataType.DateTime) {
                column = ClickHouseColumn.of("", ClickHouseDataType.DateTime64, false, 0, scaleOrLength);
            } else if (dataType == ClickHouseDataType.String) {
                column = ClickHouseColumn.of("", ClickHouseDataType.FixedString, false, scaleOrLength, 0);
            }
        }

        return column == null ? ClickHouseColumn.of("", dataType, false, false) : column;
    }

    /**
     * Converts {@link ClickHouseColumn} to {@link Class}.
     *
     * @param column  non-null column definition
     * @return non-null Java class
     */
    public static Class<?> toJavaClass(ClickHouseColumn column) {
        Class<?> clazz;
        ClickHouseDataType type = column.getDataType();
        switch (type) {
            case DateTime:
            case DateTime32:
            case DateTime64:
                clazz = column.getTimeZone() != null ? OffsetDateTime.class : LocalDateTime.class;
                break;
            default:
                clazz = type.getObjectClass();
                break;
        }
        return clazz;
    }

    /**
     * Converts {@link ClickHouseColumn} to native type.
     *
     * @param column non-null column definition
     * @return non-null native type
     */
    public static String toNativeType(ClickHouseColumn column) {
        return column.getOriginalTypeName();
    }

    /**
     * Converts {@link ClickHouseColumn} to generic SQL type defined in JDBC.
     *
     * @param column  non-null column definition
     * @return generic SQL type defined in JDBC
     */
    public static int toSqlType(ClickHouseColumn column) {
        int sqlType = Types.OTHER;
        switch (column.getDataType()) {
            case Bool:
                sqlType = Types.BOOLEAN;
                break;
            case Int8:
                sqlType = Types.TINYINT;
                break;
            case UInt8:
            case Int16:
                sqlType = Types.SMALLINT;
                break;
            case UInt16:
            case Int32:
                sqlType = Types.INTEGER;
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
            case IntervalMicrosecond:
            case IntervalMillisecond:
            case IntervalNanosecond:
            case Int64:
                sqlType = Types.BIGINT;
                break;
            case UInt64:
            case Int128:
            case UInt128:
            case Int256:
            case UInt256:
                sqlType = Types.NUMERIC;
                break;
            case Float32:
                sqlType = Types.FLOAT;
                break;
            case Float64:
                sqlType = Types.DOUBLE;
                break;
            case Decimal:
            case Decimal32:
            case Decimal64:
            case Decimal128:
            case Decimal256:
                sqlType = Types.DECIMAL;
                break;
            case Date:
            case Date32:
                sqlType = Types.DATE;
                break;
            case DateTime:
            case DateTime32:
            case DateTime64:
                sqlType = column.getTimeZone() != null ? Types.TIMESTAMP_WITH_TIMEZONE : Types.TIMESTAMP;
                break;
            case Enum8:
            case Enum16:
            case IPv4:
            case IPv6:
            case FixedString:
            case JSON:
            case Object:
            case String:
            case UUID:
                sqlType = Types.VARCHAR;
                break;
            case Point:
            case Ring:
            case Polygon:
            case MultiPolygon:
            case Array:
                sqlType = Types.ARRAY;
                break;
            case Map: // Map<?,?>
            case Nested: // Object[][]
            case Tuple: // List<?>
                sqlType = Types.STRUCT;
                break;
            case Nothing:
                sqlType = Types.NULL;
                break;
            default:
                break;
        }

        return sqlType;
    }
}

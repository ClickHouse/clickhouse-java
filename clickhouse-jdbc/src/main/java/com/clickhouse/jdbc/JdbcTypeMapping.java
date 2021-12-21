package com.clickhouse.jdbc;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.Map;

import com.clickhouse.client.ClickHouseColumn;
import com.clickhouse.client.ClickHouseDataType;

public final class JdbcTypeMapping {
    /**
     * Gets corresponding JDBC type for the given Java class.
     *
     * @param javaClass non-null Java class
     * @return JDBC type
     */
    public static int toJdbcType(Class<?> javaClass) {
        int sqlType = Types.OTHER;
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
        } else if (javaClass.isArray()) {
            sqlType = Types.ARRAY;
        }
        return sqlType;
    }

    /**
     * Gets corresponding JDBC type for the given column.
     *
     * @param column non-null column definition
     * @return JDBC type
     */
    public static int toJdbcType(Map<String, Class<?>> typeMap, ClickHouseColumn column) {
        if (typeMap != null && !typeMap.isEmpty()) {
            Class<?> javaClass = typeMap.get(column.getOriginalTypeName());
            if (javaClass == null) {
                javaClass = typeMap.get(column.getDataType().name());
            }

            if (javaClass != null) {
                return toJdbcType(javaClass);
            }
        }

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
            case Enum:
            case Enum8:
            case Enum16:
            case IPv4:
            case IPv6:
            case FixedString:
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
            case Tuple:
            case Nested:
                sqlType = Types.STRUCT;
                break;
            case Nothing:
                sqlType = Types.NULL;
                break;
            case Map:
            default:
                break;
        }

        return sqlType;
    }

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

    public static ClickHouseColumn fromJdbcType(int jdbcType, int scaleOrLength) {
        ClickHouseDataType dataType = fromJdbcType(jdbcType);
        ClickHouseColumn column = null;
        if (scaleOrLength > 0) {
            if (jdbcType == Types.NUMERIC || jdbcType == Types.DECIMAL) {
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

    public static ClickHouseDataType fromJdbcType(int jdbcType) {
        ClickHouseDataType dataType;

        switch (jdbcType) {
            case Types.BIT:
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
                dataType = ClickHouseDataType.Nested;
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

    private JdbcTypeMapping() {
    }
}

package com.clickhouse.jdbc;

import java.sql.Types;

import com.clickhouse.client.ClickHouseColumn;
import com.clickhouse.client.ClickHouseDataType;

public final class JdbcTypeMapping {
    /**
     * Gets corresponding JDBC type for the given column.
     *
     * @param column non-null column definition
     * @return JDBC type
     */
    public static int toJdbcType(ClickHouseColumn column) {
        int sqlType = Types.OTHER;

        switch (column.getDataType()) {
        case Enum:
        case Enum8:
        case Int8:
            sqlType = Types.TINYINT;
            break;
        case UInt8:
        case Enum16:
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
        case Nested:
            sqlType = Types.STRUCT;
            break;
        case Nothing:
            sqlType = Types.NULL;
            break;
        case Map:
        case Tuple:
        default:
            break;
        }

        return sqlType;
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

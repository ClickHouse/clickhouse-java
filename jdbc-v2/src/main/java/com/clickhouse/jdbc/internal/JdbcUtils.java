package com.clickhouse.jdbc.internal;

import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.jdbc.types.Array;
import com.google.common.collect.ImmutableMap;

import java.awt.*;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class JdbcUtils {
    //Define a map to store the mapping between ClickHouse data types and SQL data types
    public static final Map<ClickHouseDataType, SQLType> CLICKHOUSE_TO_SQL_TYPE_MAP = generateTypeMap();

    public static final Map<String, SQLType> CLICKHOUSE_TYPE_NAME_TO_SQL_TYPE_MAP = Collections.unmodifiableMap(generateTypeMap().entrySet()
            .stream().collect(
                HashMap::new,
                (map, entry) -> map.put(entry.getKey().name(), entry.getValue()),
                HashMap::putAll
            ));

    private static Map<ClickHouseDataType, SQLType> generateTypeMap() {
        Map<ClickHouseDataType, SQLType> map = new TreeMap<>(); // TreeMap is used to sort the keys in natural order so FixedString will be before String :-) (type match should be more accurate)
        map.put(ClickHouseDataType.Int8, JDBCType.TINYINT);
        map.put(ClickHouseDataType.Int16, JDBCType.SMALLINT);
        map.put(ClickHouseDataType.Int32, JDBCType.INTEGER);
        map.put(ClickHouseDataType.Int64, JDBCType.BIGINT);
        map.put(ClickHouseDataType.Int128, JDBCType.OTHER);
        map.put(ClickHouseDataType.Int256, JDBCType.OTHER);
        map.put(ClickHouseDataType.UInt8, JDBCType.SMALLINT);
        map.put(ClickHouseDataType.UInt16, JDBCType.INTEGER);
        map.put(ClickHouseDataType.UInt32, JDBCType.BIGINT);
        map.put(ClickHouseDataType.UInt64, JDBCType.OTHER);
        map.put(ClickHouseDataType.UInt128, JDBCType.OTHER);
        map.put(ClickHouseDataType.UInt256, JDBCType.OTHER);
        map.put(ClickHouseDataType.Float32, JDBCType.FLOAT);
        map.put(ClickHouseDataType.Float64, JDBCType.DOUBLE);
        map.put(ClickHouseDataType.Bool, JDBCType.BOOLEAN);
        map.put(ClickHouseDataType.Decimal, JDBCType.DECIMAL);
        map.put(ClickHouseDataType.Decimal32, JDBCType.DECIMAL);
        map.put(ClickHouseDataType.Decimal64, JDBCType.DECIMAL);
        map.put(ClickHouseDataType.Decimal128, JDBCType.DECIMAL);
        map.put(ClickHouseDataType.String, JDBCType.VARCHAR);
        map.put(ClickHouseDataType.FixedString, JDBCType.VARCHAR);
        map.put(ClickHouseDataType.Enum, JDBCType.VARCHAR);
        map.put(ClickHouseDataType.Enum8, JDBCType.VARCHAR);
        map.put(ClickHouseDataType.Enum16, JDBCType.VARCHAR);
        map.put(ClickHouseDataType.Date, JDBCType.DATE);
        map.put(ClickHouseDataType.Date32, JDBCType.DATE);
        map.put(ClickHouseDataType.DateTime, JDBCType.TIMESTAMP);
        map.put(ClickHouseDataType.DateTime32, JDBCType.TIMESTAMP);
        map.put(ClickHouseDataType.DateTime64, JDBCType.TIMESTAMP);
        map.put(ClickHouseDataType.Array, JDBCType.ARRAY);
        map.put(ClickHouseDataType.Nested, JDBCType.ARRAY);
        map.put(ClickHouseDataType.Map, JDBCType.JAVA_OBJECT);
        map.put(ClickHouseDataType.Point, JDBCType.OTHER);
        map.put(ClickHouseDataType.Ring, JDBCType.OTHER);
        map.put(ClickHouseDataType.Polygon, JDBCType.OTHER);
        map.put(ClickHouseDataType.LineString, JDBCType.OTHER);
        map.put(ClickHouseDataType.MultiPolygon, JDBCType.OTHER);
        map.put(ClickHouseDataType.MultiLineString, JDBCType.OTHER);
        return ImmutableMap.copyOf(map);
    }

    public static final Map<SQLType, Class<?>> SQL_TYPE_TO_CLASS_MAP = generateClassMap();
    private static Map<SQLType, Class<?>> generateClassMap() {
        Map<SQLType, Class<?>> map = new HashMap<>();
        map.put(JDBCType.CHAR, String.class);
        map.put(JDBCType.VARCHAR, String.class);
        map.put(JDBCType.LONGVARCHAR, String.class);
        map.put(JDBCType.NUMERIC, java.math.BigDecimal.class);
        map.put(JDBCType.DECIMAL, java.math.BigDecimal.class);
        map.put(JDBCType.BIT, Boolean.class);
        map.put(JDBCType.BOOLEAN, Boolean.class);
        map.put(JDBCType.TINYINT, Byte.class);
        map.put(JDBCType.SMALLINT, Short.class);
        map.put(JDBCType.INTEGER, Integer.class);
        map.put(JDBCType.BIGINT, Long.class);
        map.put(JDBCType.REAL, Float.class);
        map.put(JDBCType.FLOAT, Double.class);
        map.put(JDBCType.DOUBLE, Double.class);
        map.put(JDBCType.BINARY, byte[].class);
        map.put(JDBCType.VARBINARY, byte[].class);
        map.put(JDBCType.LONGVARBINARY, byte[].class);
        map.put(JDBCType.DATE, Date.class);
        map.put(JDBCType.TIME, java.sql.Time.class);
        map.put(JDBCType.TIMESTAMP, java.sql.Timestamp.class);
        map.put(JDBCType.TIME_WITH_TIMEZONE, java.sql.Time.class);
        map.put(JDBCType.TIMESTAMP_WITH_TIMEZONE, java.sql.Timestamp.class);
        map.put(JDBCType.CLOB, java.sql.Clob.class);
        map.put(JDBCType.BLOB, java.sql.Blob.class);
        map.put(JDBCType.ARRAY, java.sql.Array.class);
        map.put(JDBCType.STRUCT, java.sql.Struct.class);
        map.put(JDBCType.REF, java.sql.Ref.class);
        map.put(JDBCType.DATALINK, java.net.URL.class);
        map.put(JDBCType.ROWID, java.sql.RowId.class);
        map.put(JDBCType.NCHAR, String.class);
        map.put(JDBCType.NVARCHAR, String.class);
        map.put(JDBCType.LONGNVARCHAR, String.class);
        map.put(JDBCType.NCLOB, java.sql.NClob.class);
        map.put(JDBCType.SQLXML, java.sql.SQLXML.class);
        return ImmutableMap.copyOf(map);
    }

    public static final Map<ClickHouseDataType, Class<?>> DATA_TYPE_CLASS_MAP = getDataTypeClassMap();
    private static Map<ClickHouseDataType, Class<?>> getDataTypeClassMap() {
        Map<ClickHouseDataType, Class<?>> map = new HashMap<>();
        for (Map.Entry<ClickHouseDataType, SQLType> e : CLICKHOUSE_TO_SQL_TYPE_MAP.entrySet()) {
            if (e.getValue().equals(JDBCType.OTHER)) {
                switch (e.getKey()) {
                    case UInt64:
                        map.put(e.getKey(), BigInteger.class);
                        break;
                    case UInt128:
                        map.put(e.getKey(), BigInteger.class);
                        break;
                    case UInt256:
                        map.put(e.getKey(), BigInteger.class);
                        break;
                    case Int128:
                        map.put(e.getKey(), BigInteger.class);
                        break;
                    case Int256:
                        map.put(e.getKey(), BigInteger.class);
                        break;
                    case Point:
                        map.put(e.getKey(), double[].class);
                        break;
                    case LineString:
                    case Ring:
                        map.put(e.getKey(), double[][].class);
                        break;
                    case Polygon:
                    case MultiLineString:
                        map.put(e.getKey(), double[][][].class);
                        break;
                    case MultiPolygon:
                        map.put(e.getKey(), double[][][][].class);
                        break;
                    default:
                        map.put(e.getKey(), Object.class);
                }
            } else {
                map.put(e.getKey(), SQL_TYPE_TO_CLASS_MAP.get(e.getValue()));
            }
        }
        return map;
    }

    public static SQLType convertToSqlType(ClickHouseDataType clickhouseType) {
        if (clickhouseType == null) {
            return JDBCType.OTHER;
        }

        return CLICKHOUSE_TO_SQL_TYPE_MAP.getOrDefault(clickhouseType, JDBCType.OTHER);
    }

    public static Class<?> convertToJavaClass(ClickHouseDataType clickhouseType) {
        return DATA_TYPE_CLASS_MAP.get(clickhouseType);
    }

    public static Object convert(Object value, Class<?> type) throws SQLException {
        return convert(value, type, null);
    }

    public static Object convert(Object value, Class<?> type, ClickHouseColumn column) throws SQLException {
        if (value == null || type == null) {
            return value;
        }
        try {
            if (type.isInstance(value)) {
                return value;
            } else if (type == String.class) {
                return value.toString();
            } else if (type == Boolean.class || type == boolean.class) {
                return Boolean.parseBoolean(value.toString());
            } else if (type == Byte.class || type == byte.class) {
                return Byte.parseByte(value.toString());
            } else if (type == Short.class || type == short.class) {
                return Short.parseShort(value.toString());
            } else if (type == Integer.class || type == int.class) {
                return Integer.parseInt(value.toString());
            } else if (type == Long.class || type == long.class) {
                return Long.parseLong(value.toString());
            } else if (type == Float.class || type == float.class) {
                return Float.parseFloat(value.toString());
            } else if (type == Double.class || type == double.class) {
                return Double.parseDouble(value.toString());
            } else if (type == java.math.BigDecimal.class) {
                return new java.math.BigDecimal(value.toString());
            } else if (type == byte[].class) {
                return value.toString().getBytes();
            } else if (type == LocalDate.class && value instanceof TemporalAccessor) {
                return LocalDate.from((TemporalAccessor) value);
            } else if (type == LocalDateTime.class && value instanceof TemporalAccessor) {
                return LocalDateTime.from((TemporalAccessor) value);
            } else if (type == OffsetDateTime.class && value instanceof TemporalAccessor) {
                return OffsetDateTime.from((TemporalAccessor) value);
            } else if (type == ZonedDateTime.class && value instanceof TemporalAccessor) {
                return ZonedDateTime.from((TemporalAccessor) value);
            } else if (type == Date.class && value instanceof TemporalAccessor) {
                return Date.valueOf(LocalDate.from((TemporalAccessor) value));
            } else if (type == java.sql.Timestamp.class && value instanceof TemporalAccessor) {
                return java.sql.Timestamp.valueOf(LocalDateTime.from((TemporalAccessor) value));
            } else if (type == java.sql.Time.class && value instanceof TemporalAccessor) {
                return java.sql.Time.valueOf(LocalTime.from((TemporalAccessor) value));
            } else if (type == java.sql.Array.class && value instanceof BinaryStreamReader.ArrayValue) {//It's cleaner to use getList but this handles the more generic getObject
                if (column != null && column.getArrayBaseColumn() != null) {
                    return new Array(convertList(((BinaryStreamReader.ArrayValue) value).asList(), JdbcUtils.convertToJavaClass(column.getArrayBaseColumn().getDataType())), "Object", JDBCType.JAVA_OBJECT.getVendorTypeNumber());
                }
                return new Array(((BinaryStreamReader.ArrayValue) value).asList(), "Object", JDBCType.JAVA_OBJECT.getVendorTypeNumber());
            } else if (type == java.sql.Array.class && value instanceof List<?>) {
                if (column != null && column.getArrayBaseColumn() != null) {
                    return new Array(convertList(((List) value), JdbcUtils.convertToJavaClass(column.getArrayBaseColumn().getDataType())), "Object", JDBCType.JAVA_OBJECT.getVendorTypeNumber());
                }
                return new Array((List) value, "Object", JDBCType.JAVA_OBJECT.getVendorTypeNumber());
            } else if (type == Inet4Address.class && value instanceof Inet6Address) {
                // Convert Inet6Address to Inet4Address
                return Inet4Address.getByName(value.toString());
            } else if (type == Inet6Address.class && value instanceof Inet4Address) {
                // Convert Inet4Address to Inet6Address
                return Inet6Address.getByName(value.toString());
            }
        } catch (Exception e) {
            throw new SQLException("Failed to convert " + value + " to " + type.getName(), ExceptionUtils.SQL_STATE_DATA_EXCEPTION);
        }

        throw new SQLException("Unsupported conversion from " + value.getClass().getName() + " to " + type.getName(), ExceptionUtils.SQL_STATE_DATA_EXCEPTION);
    }

    public static List<Object> convertList(List<Object> values, Class<?> type) throws SQLException {
        if (values == null || type == null) {
            return values;
        }

        List<Object> convertedValues = new ArrayList<>(values.size());
        for (Object value : values) {
            convertedValues.add(convert(value, type));
        }
        return convertedValues;
    }
}

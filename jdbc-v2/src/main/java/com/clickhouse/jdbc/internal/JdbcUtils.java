package com.clickhouse.jdbc.internal;

import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.jdbc.types.Array;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.SQLType;
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
    private static final Map<ClickHouseDataType, SQLType> CLICKHOUSE_TO_SQL_TYPE_MAP = generateTypeMap();

    public static final Map<String, SQLType> CLICKHOUSE_TYPE_NAME_TO_SQL_TYPE_MAP = Collections.unmodifiableMap(generateTypeMap().entrySet()
            .stream().collect(
                HashMap::new,
                (map, entry) -> map.put(entry.getKey().name(), entry.getValue()),
                HashMap::putAll
            ));

    private static Map<ClickHouseDataType, SQLType> generateTypeMap() {
        Map<ClickHouseDataType, SQLType> map = new TreeMap<>(); // TreeMap is used to sort the keys in natural order so FixedString will be before String :-) (type match should be more accurate)
        map.put(ClickHouseDataType.Int8, JDBCType.TINYINT);
        map.put(ClickHouseDataType.UInt8, JDBCType.TINYINT);
        map.put(ClickHouseDataType.Int16, JDBCType.SMALLINT);
        map.put(ClickHouseDataType.UInt16, JDBCType.SMALLINT);
        map.put(ClickHouseDataType.Int32, JDBCType.INTEGER);
        map.put(ClickHouseDataType.UInt32, JDBCType.INTEGER);
        map.put(ClickHouseDataType.Int64, JDBCType.BIGINT);
        map.put(ClickHouseDataType.UInt64, JDBCType.BIGINT);
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
        return map;
    }

    private static final Map<SQLType, Class<?>> SQL_TYPE_TO_CLASS_MAP = generateClassMap();
    private static Map<SQLType, Class<?>> generateClassMap() {
        Map<SQLType, Class<?>> map = new HashMap<>();
        map.put(JDBCType.CHAR, String.class);
        map.put(JDBCType.VARCHAR, String.class);
        map.put(JDBCType.LONGVARCHAR, String.class);
        map.put(JDBCType.NUMERIC, java.math.BigDecimal.class);
        map.put(JDBCType.DECIMAL, java.math.BigDecimal.class);
        map.put(JDBCType.BIT, Boolean.class);
        map.put(JDBCType.BOOLEAN, Boolean.class);
        map.put(JDBCType.TINYINT, Integer.class);
        map.put(JDBCType.SMALLINT, Integer.class);
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
        return map;
    }

    public static SQLType convertToSqlType(ClickHouseDataType clickhouseType) {
        if (clickhouseType == null) {
            return JDBCType.NULL;
        }

        return CLICKHOUSE_TO_SQL_TYPE_MAP.getOrDefault(clickhouseType, JDBCType.OTHER);
    }

    public static Class<?> convertToJavaClass(ClickHouseDataType clickhouseType) {
        return SQL_TYPE_TO_CLASS_MAP.get(convertToSqlType(clickhouseType));
    }

    public static List<String> tokenizeSQL(String sql) {
        List<String> tokens = new ArrayList<>();

        // Remove SQL comments
        String withoutComments = sql
                .replaceAll("--.*?$", "") // Remove single-line comments
                .replaceAll("/\\*.*?\\*/", ""); // Remove multi-line comments

        StringBuilder currentToken = new StringBuilder();
        boolean insideQuotes = false;

        for (int i = 0; i < withoutComments.length(); i++) {
            char c = withoutComments.charAt(i);

            if (c == '"') {
                insideQuotes = !insideQuotes; // Toggle the insideQuotes flag
                currentToken.append(c); // Include the quote in the token
            } else if (Character.isWhitespace(c) && !insideQuotes) {
                if (currentToken.length() > 0) {
                    tokens.add(currentToken.toString());
                    currentToken.setLength(0); // Clear the current token
                }
            } else {
                currentToken.append(c);
            }
        }

        // Add the last token if it exists
        if (currentToken.length() > 0) {
            tokens.add(currentToken.toString());
        }

        return tokens;
    }

    public static boolean isBlank(String str) {
        return str == null || str.isEmpty() || str.trim().isEmpty();
    }

    public static boolean containsIgnoresCase(List<String> list, String str) {
        if (list == null || list.isEmpty() || isBlank(str)) {
            return false;
        }

        for (String s : list) {
            if (s.equalsIgnoreCase(str)) {
                return true;
            }
        }

        return false;
    }

    public static int indexOfIgnoresCase(List<String> list, String str) {
        if (list == null || list.isEmpty() || isBlank(str)) {
            return -1;
        }

        for (int i = 0; i < list.size(); i++) {
            if (list.get(i).equalsIgnoreCase(str)) {
                return i;
            }
        }

        return -1;
    }

    public static String generateSqlTypeSizes(String columnName) {
        StringBuilder sql = new StringBuilder("multiIf(");
        sql.append("character_octet_length IS NOT NULL, character_octet_length, ");
        for (ClickHouseDataType type : ClickHouseDataType.values()) {
            if (type.getByteLength() > 0) {
                sql.append(columnName).append(" == '").append(type.name()).append("', ").append(type.getByteLength()).append(", ");
            }
        }
        sql.append("numeric_precision IS NOT NULL, numeric_precision, ");
        sql.append("0)");
        return sql.toString();
    }


    public static Object convert(Object value, Class<?> type) throws SQLException {
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
                return new Array(((BinaryStreamReader.ArrayValue) value).asList(), "Object", JDBCType.JAVA_OBJECT.getVendorTypeNumber());
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

    public static String escapeQuotes(String str) {
        if (str == null || str.isEmpty()) {
            return str;
        }
        return str
                .replace("'", "\\'")
                .replace("\"", "\\\"");
    }
}

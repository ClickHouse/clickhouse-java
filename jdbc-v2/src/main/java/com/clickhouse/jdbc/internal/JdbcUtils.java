package com.clickhouse.jdbc.internal;

import com.clickhouse.data.ClickHouseDataType;

import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.SQLType;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JdbcUtils {
    //Define a map to store the mapping between ClickHouse data types and SQL data types
    private static final Map<ClickHouseDataType, SQLType> CLICKHOUSE_TO_SQL_TYPE_MAP = generateTypeMap();
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
        map.put(ClickHouseDataType.Decimal, JDBCType.DECIMAL);
        map.put(ClickHouseDataType.Decimal32, JDBCType.DECIMAL);
        map.put(ClickHouseDataType.Decimal64, JDBCType.DECIMAL);
        map.put(ClickHouseDataType.Decimal128, JDBCType.DECIMAL);
        map.put(ClickHouseDataType.String, JDBCType.VARCHAR);
        map.put(ClickHouseDataType.FixedString, JDBCType.CHAR);
        map.put(ClickHouseDataType.Enum8, JDBCType.VARCHAR);
        map.put(ClickHouseDataType.Enum16, JDBCType.VARCHAR);
        map.put(ClickHouseDataType.Date, JDBCType.DATE);
        map.put(ClickHouseDataType.Date32, JDBCType.DATE);
        map.put(ClickHouseDataType.DateTime, JDBCType.TIMESTAMP_WITH_TIMEZONE);
        map.put(ClickHouseDataType.DateTime32, JDBCType.TIMESTAMP_WITH_TIMEZONE);
        map.put(ClickHouseDataType.DateTime64, JDBCType.TIMESTAMP_WITH_TIMEZONE);
        map.put(ClickHouseDataType.Array, JDBCType.ARRAY);
        map.put(ClickHouseDataType.Nested, JDBCType.ARRAY);
        map.put(ClickHouseDataType.Map, JDBCType.JAVA_OBJECT);
        return map;
    }

    public static SQLType convertToSqlType(ClickHouseDataType clickhouseType) {
        if (clickhouseType == null) {
            return JDBCType.NULL;
        }

        return CLICKHOUSE_TO_SQL_TYPE_MAP.getOrDefault(clickhouseType, JDBCType.OTHER);
    }


    public static String generateSqlTypeEnum(String columnName) {
        StringBuilder sql = new StringBuilder("multiIf(");
        for (ClickHouseDataType type : CLICKHOUSE_TO_SQL_TYPE_MAP.keySet()) {
            sql.append("position(").append(columnName).append(", '").append(type.name()).append("') > 0, ").append(CLICKHOUSE_TO_SQL_TYPE_MAP.get(type).getVendorTypeNumber()).append(", ");
        }
        sql.append(JDBCType.OTHER.getVendorTypeNumber()).append(")");
        return sql.toString();
    }

    public static List<String> tokenizeSQL(String sql) {
        List<String> tokens = new ArrayList<>();
        Matcher m = Pattern.compile("([^\"]\\S*|\".+?\")\\s*").matcher(sql);
        while (m.find()) {
            String token = m.group(1).replace("\"", "").trim();
            if (!token.isEmpty() && token.charAt(token.length() - 1) == ',') {
                token = token.substring(0, token.length() - 1);
            }

            if (!isBlank(token)) {
                tokens.add(token);
            }
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
            } else if (type == LocalDate.class && value instanceof TemporalAccessor) {
                return LocalDate.from((TemporalAccessor) value);
            } else if (type == LocalDateTime.class && value instanceof TemporalAccessor) {
                return LocalDateTime.from((TemporalAccessor) value);
            } else if (type == OffsetDateTime.class && value instanceof TemporalAccessor) {
                return OffsetDateTime.from((TemporalAccessor) value);
            } else if (type == ZonedDateTime.class && value instanceof TemporalAccessor) {
                return ZonedDateTime.from((TemporalAccessor) value);
            }
        } catch (Exception e) {
            throw new SQLException("Failed to convert " + value + " to " + type.getName(), ExceptionUtils.SQL_STATE_DATA_EXCEPTION);
        }

        throw new SQLException("Unsupported conversion from " + value.getClass().getName() + " to " + type.getName(), ExceptionUtils.SQL_STATE_DATA_EXCEPTION);
    }

}

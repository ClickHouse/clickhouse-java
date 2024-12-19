package com.clickhouse.jdbc.internal;

import com.clickhouse.data.ClickHouseDataType;

import java.sql.Types;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JdbcUtils {
    //Define a map to store the mapping between ClickHouse data types and SQL data types
    private static final Map<ClickHouseDataType, Integer> CLICKHOUSE_TO_SQL_TYPE_MAP = generateTypeMap();
    private static Map<ClickHouseDataType, Integer> generateTypeMap() {
        Map<ClickHouseDataType, Integer> map = new TreeMap<>(); // TreeMap is used to sort the keys in natural order so FixedString will be before String :-) (type match should be more accurate)
        map.put(ClickHouseDataType.Int8, Types.TINYINT);
        map.put(ClickHouseDataType.UInt8, Types.TINYINT);
        map.put(ClickHouseDataType.Int16, Types.SMALLINT);
        map.put(ClickHouseDataType.UInt16, Types.SMALLINT);
        map.put(ClickHouseDataType.Int32, Types.INTEGER);
        map.put(ClickHouseDataType.UInt32, Types.INTEGER);
        map.put(ClickHouseDataType.Int64, Types.BIGINT);
        map.put(ClickHouseDataType.UInt64, Types.BIGINT);
        map.put(ClickHouseDataType.Float32, Types.FLOAT);
        map.put(ClickHouseDataType.Float64, Types.DOUBLE);
        map.put(ClickHouseDataType.Decimal, Types.DECIMAL);
        map.put(ClickHouseDataType.Decimal32, Types.DECIMAL);
        map.put(ClickHouseDataType.Decimal64, Types.DECIMAL);
        map.put(ClickHouseDataType.Decimal128, Types.DECIMAL);
        map.put(ClickHouseDataType.String, Types.VARCHAR);
        map.put(ClickHouseDataType.FixedString, Types.CHAR);
        map.put(ClickHouseDataType.Enum8, Types.VARCHAR);
        map.put(ClickHouseDataType.Enum16, Types.VARCHAR);
        map.put(ClickHouseDataType.Date, Types.DATE);
        map.put(ClickHouseDataType.Date32, Types.DATE);
        map.put(ClickHouseDataType.DateTime, Types.TIMESTAMP_WITH_TIMEZONE);
        map.put(ClickHouseDataType.DateTime32, Types.TIMESTAMP_WITH_TIMEZONE);
        map.put(ClickHouseDataType.DateTime64, Types.TIMESTAMP_WITH_TIMEZONE);
        map.put(ClickHouseDataType.Array, Types.ARRAY);
        map.put(ClickHouseDataType.Nested, Types.ARRAY);
        map.put(ClickHouseDataType.Map, Types.JAVA_OBJECT);
        return map;
    }

    public static int convertToSqlType(ClickHouseDataType clickhouseType) {
        if (clickhouseType == null) {
            return Types.NULL;
        }

        return CLICKHOUSE_TO_SQL_TYPE_MAP.getOrDefault(clickhouseType, Types.OTHER);
    }

    public static String generateSqlTypeEnum(String columnName) {
        StringBuilder sql = new StringBuilder("multiIf(");
        for (ClickHouseDataType type : CLICKHOUSE_TO_SQL_TYPE_MAP.keySet()) {
            sql.append("position(").append(columnName).append(", '").append(type.name()).append("') > 0, ").append(CLICKHOUSE_TO_SQL_TYPE_MAP.get(type)).append(", ");
        }
        sql.append(Types.OTHER + ")");
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

}

package ru.yandex.clickhouse.util;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * @author Aleksandr Kormushin {@literal (<kormushin@yandex-team.ru>)}
 */
public class TypeUtils {

    public static int toSqlType(String clickshouseType) {
        if (clickshouseType.startsWith("Int") || clickshouseType.startsWith("UInt")) {
            return clickshouseType.endsWith("64") ? Types.BIGINT : Types.INTEGER;
        }
        if ("String".equals(clickshouseType)) return Types.VARCHAR;
        if (clickshouseType.startsWith("Float32")) return Types.FLOAT;
        if (clickshouseType.startsWith("Float64")) return Types.DOUBLE;
        if ("Date".equals(clickshouseType)) return Types.DATE;
        if ("DateTime".equals(clickshouseType)) return Types.TIMESTAMP;
        if ("FixedString".equals(clickshouseType)) return Types.BLOB;
        if (isArray(clickshouseType)) return Types.ARRAY;

        // don't know what to return actually
        return Types.VARCHAR;
    }

    public static boolean isUnsigned(String clickhouseType){
        return clickhouseType.startsWith("UInt");
    }

    public static int[] supportedTypes() {
        return new int[]{
                Types.BIGINT, Types.INTEGER, Types.VARCHAR, Types.FLOAT,
                Types.DATE, Types.TIMESTAMP, Types.BLOB, Types.ARRAY
        };
    }

    public static String getArrayElementTypeName(String clickhouseType) {
        if (!isArray(clickhouseType)) {
            throw new IllegalArgumentException("not an array");
        }

        return clickhouseType.substring("Array(".length(), clickhouseType.length() - 1);
    }

    private static boolean isArray(String clickhouseType) {
        return clickhouseType.startsWith("Array(")
                && clickhouseType.endsWith(")");
    }

    public static Class toClass(int sqlType, boolean isUnsigned) throws SQLException {
        return toClass(sqlType, -1, isUnsigned);
    }

    public static Class toClass(int sqlType, int elementSqltype, boolean isUnsigned) throws SQLException {
        switch (sqlType) {
            case Types.BIT:
            case Types.BOOLEAN:
                return Boolean.class;
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
                if (isUnsigned) return Long.class;
                return Integer.class;
            case Types.BIGINT:
                if (isUnsigned) return BigInteger.class;
                return Long.class;
            case Types.DOUBLE:
                return Double.class;
            case Types.NUMERIC:
            case Types.DECIMAL:
                return BigDecimal.class;
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.BLOB:
                return String.class;
            case Types.FLOAT:
            case Types.REAL:
                return Float.class;
            case Types.DATE:
                return java.sql.Date.class;
            case Types.TIMESTAMP:
                return Timestamp.class;
            case Types.TIME:
                return Time.class;
            case Types.ARRAY:
                Class elementType = toClass(elementSqltype, isUnsigned);
                return Array.newInstance(elementType, 0).getClass();
            default:
                throw new UnsupportedOperationException("Sql type " + sqlType + "is not supported");
        }
    }

}

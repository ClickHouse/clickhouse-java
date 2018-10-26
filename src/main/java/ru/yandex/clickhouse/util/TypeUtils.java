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

    public static final String NULLABLE_YES = "YES";
    public static final String NULLABLE_NO = "NO";

    public static int toSqlType(String clickshouseType) {
        if (isNullable(clickshouseType)) {
            clickshouseType = unwrapNullable(clickshouseType);
        }
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
        if ("UUID".equals(clickshouseType)) return Types.OTHER;

        // don't know what to return actually
        return Types.VARCHAR;
    }

    private static String unwrapNullable(String clickshouseType) {
        return clickshouseType.substring("Nullable(".length(), clickshouseType.length() - 1);
    }

    private static boolean isNullable(String clickshouseType) {
        return clickshouseType.startsWith("Nullable(") && clickshouseType.endsWith(")");
    }

    public static boolean isUnsigned(String clickhouseType){
        if (isNullable(clickhouseType)) {
            clickhouseType = unwrapNullable(clickhouseType);
        }
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

    public static int getColumnSize(String type) {
        if (isNullable(type)) {
            type = unwrapNullable(type);
        }

        if (type.equals("Float32")) {
            return 8;
        } else if (type.equals("Float64")) {
            return 17;
        } else if (type.equals("Int8")) {
            return 4;
        } else if (type.equals("Int16")) {
            return 6;
        } else if (type.equals("Int32")) {
            return 11;
        } else if (type.equals("Int64")) {
            return 20;
        } else if (type.equals("UInt8")) {
            return 3;
        } else if (type.equals("UInt16")) {
            return 5;
        } else if (type.equals("UInt32")) {
            return 10;
        } else if (type.equals("UInt64")) {
            return 19;
        } else if (type.equals("Date")) {
            // number of chars in '2018-01-01'
            return 10;
        } else if (type.equals("DateTime")) {
            // number of chars in '2018-01-01 01:02:35'
            return 19;
        } else if (type.startsWith("FixedString(")) {
            String numBytes = type.substring("FixedString(".length(), type.length() - 1);
            return Integer.parseInt(numBytes);
        } else {
            // size unknown
            return 0;
        }
    }

    public static int getDecimalDigits(String type) {
        if (isNullable(type)) {
            type = unwrapNullable(type);
        }

        if (type.equals("Float32")) {
            return 8;
        } else if (type.equals("Float64")) {
            return 17;
        }
        // no other types support decimal digits
        return 0;
    }

    public static String isTypeNull(String clickshouseType) {
        if(isNullable(clickshouseType)){
            return NULLABLE_YES;
        }else{
            return NULLABLE_NO;
        }
    }
}

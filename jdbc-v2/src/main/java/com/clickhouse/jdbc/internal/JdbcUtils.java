package com.clickhouse.jdbc.internal;

import com.clickhouse.client.api.DataTypeUtils;
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.data_formats.internal.InetAddressConverter;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.Tuple;
import com.clickhouse.jdbc.types.Array;
import com.google.common.collect.ImmutableMap;

import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.Time;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAccessor;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Function;

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
        map.put(ClickHouseDataType.BFloat16, JDBCType.FLOAT);
        map.put(ClickHouseDataType.Bool, JDBCType.BOOLEAN);
        map.put(ClickHouseDataType.Decimal, JDBCType.DECIMAL);
        map.put(ClickHouseDataType.Decimal32, JDBCType.DECIMAL);
        map.put(ClickHouseDataType.Decimal64, JDBCType.DECIMAL);
        map.put(ClickHouseDataType.Decimal128, JDBCType.DECIMAL);
        map.put(ClickHouseDataType.Decimal256, JDBCType.DECIMAL);
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
        map.put(ClickHouseDataType.Time, JDBCType.TIME);
        map.put(ClickHouseDataType.Time64, JDBCType.TIME);
        map.put(ClickHouseDataType.Array, JDBCType.ARRAY);
        map.put(ClickHouseDataType.Nested, JDBCType.ARRAY);
        map.put(ClickHouseDataType.Map, JDBCType.OTHER);
        map.put(ClickHouseDataType.Point, JDBCType.ARRAY);
        map.put(ClickHouseDataType.Ring, JDBCType.ARRAY);
        map.put(ClickHouseDataType.Polygon, JDBCType.ARRAY);
        map.put(ClickHouseDataType.LineString, JDBCType.ARRAY);
        map.put(ClickHouseDataType.MultiPolygon, JDBCType.ARRAY);
        map.put(ClickHouseDataType.MultiLineString, JDBCType.ARRAY);
        map.put(ClickHouseDataType.Geometry, JDBCType.ARRAY);
        map.put(ClickHouseDataType.Tuple, JDBCType.OTHER);
        map.put(ClickHouseDataType.Nothing, JDBCType.OTHER);
        map.put(ClickHouseDataType.UUID, JDBCType.OTHER);
        map.put(ClickHouseDataType.IPv6, JDBCType.OTHER);
        map.put(ClickHouseDataType.IPv4, JDBCType.OTHER);
        map.put(ClickHouseDataType.IntervalNanosecond, JDBCType.BIGINT);
        map.put(ClickHouseDataType.IntervalMillisecond, JDBCType.BIGINT);
        map.put(ClickHouseDataType.IntervalMicrosecond, JDBCType.BIGINT);
        map.put(ClickHouseDataType.IntervalSecond, JDBCType.BIGINT);
        map.put(ClickHouseDataType.IntervalMinute, JDBCType.BIGINT);
        map.put(ClickHouseDataType.IntervalHour, JDBCType.BIGINT);
        map.put(ClickHouseDataType.IntervalDay, JDBCType.BIGINT);
        map.put(ClickHouseDataType.IntervalMonth, JDBCType.BIGINT);
        map.put(ClickHouseDataType.IntervalWeek, JDBCType.BIGINT);
        map.put(ClickHouseDataType.IntervalQuarter, JDBCType.BIGINT);
        map.put(ClickHouseDataType.IntervalYear, JDBCType.BIGINT);
        map.put(ClickHouseDataType.JSON, JDBCType.OTHER);
        map.put(ClickHouseDataType.Object, JDBCType.OTHER);
        map.put(ClickHouseDataType.LowCardinality, JDBCType.OTHER);
        map.put(ClickHouseDataType.Nullable, JDBCType.OTHER);
        map.put(ClickHouseDataType.SimpleAggregateFunction, JDBCType.OTHER);
        map.put(ClickHouseDataType.AggregateFunction, JDBCType.OTHER);
        map.put(ClickHouseDataType.Variant, JDBCType.OTHER);
        map.put(ClickHouseDataType.Dynamic, JDBCType.OTHER);
        map.put(ClickHouseDataType.QBit, JDBCType.OTHER);


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
        map.put(JDBCType.FLOAT, Float.class);
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

    public static final Set<ClickHouseDataType> INVALID_TARGET_TYPES = EnumSet.of(ClickHouseDataType.Nested, ClickHouseDataType.Enum8, ClickHouseDataType.Enum16, ClickHouseDataType.Enum,
            ClickHouseDataType.Tuple, ClickHouseDataType.Map, ClickHouseDataType.Nothing, ClickHouseDataType.Nullable, ClickHouseDataType.Variant);

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
                    case UUID:
                        map.put(e.getKey(), UUID.class);
                        break;
                    case IPv4:
                    case IPv6:
                        // should be mapped to Object because require conversion.
                    default:
                        map.put(e.getKey(), Object.class);
                }
            } else if (e.getValue().equals(JDBCType.STRUCT)) {
                map.put(e.getKey(), Object.class);
            } else {
                map.put(e.getKey(), SQL_TYPE_TO_CLASS_MAP.get(e.getValue()));
            }
        }
        return map;
    }

    public static final Map<SQLType, ClickHouseDataType> SQL_TO_CLICKHOUSE_TYPE_MAP = createSQLToClickHouseDataTypeMap();

    private static Map<SQLType, ClickHouseDataType> createSQLToClickHouseDataTypeMap() {
        Map<SQLType, ClickHouseDataType> map = new HashMap<>();
        map.put(JDBCType.TINYINT, ClickHouseDataType.Int8);
        map.put(JDBCType.SMALLINT, ClickHouseDataType.Int16);
        map.put(JDBCType.INTEGER, ClickHouseDataType.Int32);
        map.put(JDBCType.BIGINT, ClickHouseDataType.Int64);
        map.put(JDBCType.FLOAT, ClickHouseDataType.Float32);
        map.put(JDBCType.REAL, ClickHouseDataType.Float32);
        map.put(JDBCType.DOUBLE, ClickHouseDataType.Float64);
        map.put(JDBCType.BOOLEAN, ClickHouseDataType.Bool);
        map.put(JDBCType.DATE, ClickHouseDataType.Date32);
        map.put(JDBCType.TIME, ClickHouseDataType.Time);
        map.put(JDBCType.TIMESTAMP, ClickHouseDataType.DateTime64);
        map.put(JDBCType.TIMESTAMP_WITH_TIMEZONE, ClickHouseDataType.DateTime64);
        map.put(JDBCType.BINARY, ClickHouseDataType.String);
        map.put(JDBCType.VARBINARY, ClickHouseDataType.String);
        map.put(JDBCType.LONGVARBINARY, ClickHouseDataType.String);
        map.put(JDBCType.CHAR, ClickHouseDataType.String);
        map.put(JDBCType.NCHAR, ClickHouseDataType.String);
        map.put(JDBCType.VARCHAR, ClickHouseDataType.String);
        map.put(JDBCType.LONGNVARCHAR, ClickHouseDataType.String);
        map.put(JDBCType.NVARCHAR, ClickHouseDataType.String);
        map.put(JDBCType.DECIMAL, ClickHouseDataType.Decimal32);
        map.put(JDBCType.ARRAY, ClickHouseDataType.Array);
        return Collections.unmodifiableMap(map);
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

    private static Class<?> unwrapPrimitiveType(Class<?> type) {
        if (type == int.class) {
            return Integer.class;
        } else if (type == long.class) {
            return Long.class;
        } else if (type == boolean.class) {
            return Boolean.class;
        } else if (type == float.class) {
            return Float.class;
        } else if (type == double.class) {
            return Double.class;
        } else if (type == char.class) {
            return Character.class;
        } else if (type == byte.class) {
            return Byte.class;
        } else if (type == short.class) {
            return Short.class;
        }
        return type;
    }

    public static Object convert(Object value, Class<?> type) throws SQLException {
        return convert(value, type, null);
    }

    public static Object convert(Object value, Class<?> type, ClickHouseColumn column) throws SQLException {
        if (value == null || type == null) {
            return value;
        }

        type = unwrapPrimitiveType(type);
        if (type.isInstance(value)) {
            return value;
        }

        if (value instanceof List<?>) {
            List<?> listValue = (List<?>) value;
            if (type != java.sql.Array.class) {
                return convertList(listValue, type, column.getArrayNestedLevel());
            }

            if (column != null && column.getArrayBaseColumn() != null) {
                ClickHouseDataType baseType = column.getArrayBaseColumn().getDataType();
                Object[] convertedValues = convertList(listValue, convertToJavaClass(baseType),
                        column.getArrayNestedLevel());
                return new Array(column, convertedValues);
            }

            // base type is unknown. all objects should be converted
            return new Array(column, listValue.toArray());
        }

        if (value.getClass().isArray()) {
            if (type == java.sql.Array.class) {
                return new Array(column, arrayToObjectArray(value));
            } else if (type == Tuple.class) {
                return new Tuple(true, value);
            }
        }

        if (type == java.sql.Array.class && value instanceof BinaryStreamReader.ArrayValue) {
            BinaryStreamReader.ArrayValue arrayValue = (BinaryStreamReader.ArrayValue) value;

            if (column != null && column.getArrayBaseColumn() != null) {
                ClickHouseDataType baseType = column.getArrayBaseColumn().getDataType();
                Object[] convertedValues = convertArray(arrayValue.getArray(), convertToJavaClass(baseType),
                        column.getArrayNestedLevel());
                return new Array(column, convertedValues);
            }

            return new Array(column, arrayValue.getArrayOfObjects());
        }

        return convertObject(value, type, column);
    }

    static Object convertObject(Object value, Class<?> type, ClickHouseColumn column) throws SQLException {
        if (value == null || type == null) {
            return value;
        }
        try {
            if (type == String.class) {
                return value.toString();
            } else if (type == Boolean.class) {
                String str = value.toString();
                return !("false".equalsIgnoreCase(str) || "0".equalsIgnoreCase(str));
            } else if (type == Byte.class) {
                return Byte.parseByte(value.toString());
            } else if (type == Short.class) {
                return Short.parseShort(value.toString());
            } else if (type == Integer.class) {
                return Integer.parseInt(value.toString());
            } else if (type == Long.class) {
                return Long.parseLong(value.toString());
            } else if (type == Float.class) {
                return Float.parseFloat(value.toString());
            } else if (type == Double.class) {
                return Double.parseDouble(value.toString());
            } else if (type == java.math.BigDecimal.class) {
                return new java.math.BigDecimal(value.toString());
            } else if (type == Duration.class && value instanceof LocalDateTime) {
                return DataTypeUtils.localDateTimeToDuration((LocalDateTime) value);
            } else if (value instanceof TemporalAccessor) {
                TemporalAccessor temporalValue = (TemporalAccessor) value;
                if (type == LocalDate.class) {
                    return LocalDate.from(temporalValue);
                } else if (type == LocalDateTime.class) {
                    return LocalDateTime.from(temporalValue);
                } else if (type == OffsetDateTime.class) {
                    return OffsetDateTime.from(temporalValue);
                } else if (type == LocalTime.class) {
                    return LocalTime.from(temporalValue);
                } else if (type == ZonedDateTime.class) {
                    return ZonedDateTime.from(temporalValue);
                } else if (type == Instant.class) {
                    return Instant.from(temporalValue);
                } else if (type == Date.class) {
                    return Date.valueOf(LocalDate.from(temporalValue));
                } else if (type == java.sql.Timestamp.class) {
                    return java.sql.Timestamp.valueOf(LocalDateTime.from(temporalValue));
                } else if (type == java.sql.Time.class) {
                    return java.sql.Time.valueOf(LocalTime.from(temporalValue));
                }
            } else if (type == Time.class && value instanceof Integer) { // Time
                return new Time((Integer) value * 1000L);
            } else if (type == Time.class && value instanceof Long) { // Time64
                Instant instant = DataTypeUtils.instantFromTime64Integer(column.getScale(), (Long) value);
                return new Time(instant.getEpochSecond() * 1000L + instant.getNano() / 1_000_000);
            } else if (type == Inet4Address.class && value instanceof Inet6Address) {
                // Convert Inet6Address to Inet4Address
                return InetAddressConverter.convertToIpv4((InetAddress) value);
            } else if (type == Inet6Address.class && value instanceof Inet4Address) {
                // Convert Inet4Address to Inet6Address
                return InetAddressConverter.convertToIpv6((InetAddress) value);
            }
        } catch (Exception e) {
            throw new SQLException("Failed to convert from " + value.getClass().getName() + " to " + type.getName(), ExceptionUtils.SQL_STATE_DATA_EXCEPTION, e);
        }

        throw new SQLException("Unsupported conversion from " + value.getClass().getName() + " to " + type.getName(), ExceptionUtils.SQL_STATE_DATA_EXCEPTION);
    }

    public static <T> T[] convertList(List<?> values, Class<T> type, int dimensions) throws SQLException {
        if (values == null) {
            return null;
        }

        if (dimensions <= 0) {
            throw new IllegalArgumentException("Cannot convert list to array with less then 1D");
        }

        int[] arrayDimensions = new int[dimensions];
        arrayDimensions[0] = values.size();
        T[] convertedValues = (T[]) java.lang.reflect.Array.newInstance(type, arrayDimensions);
        Stack<ArrayProcessingCursor> stack = new Stack<>();
        stack.push(new ArrayProcessingCursor(convertedValues, values,  values.size(), dimensions));
        while (!stack.isEmpty()) {
            ArrayProcessingCursor cursor = stack.pop();

            for (int i = 0; i < cursor.size; i++) {
                Object value = cursor.getValue(i);
                if (value == null) {
                    continue; // no need to set null value
                } else  if (value instanceof List<?>) {
                    List<?> srcList = (List<?>) value;
                    int depth = cursor.depth - 1;
                    if (depth <= 0) {
                        throw new IllegalStateException("There is a child array at depth 0 where it is not expected");
                    }
                    arrayDimensions = new int[depth];
                    arrayDimensions[0] = srcList.size();
                    T[] targetArray = (T[]) java.lang.reflect.Array.newInstance(type, arrayDimensions);
                    stack.push(new ArrayProcessingCursor(targetArray, value,  srcList.size(), depth));
                    java.lang.reflect.Array.set(cursor.targetArray, i, targetArray);
                } else {
                    java.lang.reflect.Array.set(cursor.targetArray, i, convert(value, type));
                }
            }
        }

        return convertedValues;
    }

    /**
     * Convert array to java array and all its elements
     * @param values
     * @param type
     * @param dimensions
     * @return
     * @param <T>
     * @throws SQLException
     */
    public static <T> T[] convertArray(Object values, Class<T> type, int dimensions) throws SQLException {
        if (values == null) {
            return null;
        }

        if (dimensions <= 0) {
            throw new IllegalArgumentException("Cannot convert list to array with less then 1D");
        }

        int[] arrayDimensions = new int[dimensions];
        arrayDimensions[0] = java.lang.reflect.Array.getLength(values);
        T[] convertedValues = (T[]) java.lang.reflect.Array.newInstance(type, arrayDimensions);
        Stack<ArrayProcessingCursor> stack = new Stack<>();
        stack.push(new ArrayProcessingCursor(convertedValues, values,  arrayDimensions[0], dimensions));

        while (!stack.isEmpty()) {
            ArrayProcessingCursor cursor = stack.pop();

            for (int i = 0; i < cursor.size; i++) {
                Object value = cursor.getValue(i);
                if (value == null) {
                    continue; // no need to set null value
                } else  if (value.getClass().isArray()) {
                    int depth = cursor.depth - 1;
                    if (depth <= 0) {
                        throw new IllegalStateException("There is a child array at depth 0 where it is not expected");
                    }
                    arrayDimensions = new int[depth];
                    arrayDimensions[0] = java.lang.reflect.Array.getLength(value);
                    T[] targetArray = (T[]) java.lang.reflect.Array.newInstance(type, arrayDimensions);
                    stack.push(new ArrayProcessingCursor(targetArray, value,  arrayDimensions[0], depth));
                    java.lang.reflect.Array.set(cursor.targetArray, i, targetArray);
                } else {
                    java.lang.reflect.Array.set(cursor.targetArray, i, convert(value, type));
                }
            }
        }

        return convertedValues;
    }

    private static final class ArrayProcessingCursor {
        private final Object targetArray;
        private final int size;
        private final Function<Integer, Object> valueGetter;
        private final int depth;

        public  ArrayProcessingCursor(Object targetArray, Object srcArray, int size, int depth) {
            this.targetArray = targetArray;
            this.size = size;
            this.depth = depth;
            if (srcArray instanceof List<?>) {
                List<?> list = (List<?>)  srcArray;
                this.valueGetter = list::get;
            } else {
                this.valueGetter = (i) -> java.lang.reflect.Array.get(srcArray, i);
            }
        }

        public Object getValue(int i) {
            return valueGetter.apply(i);
        }
    }

    public static Object[] arrayToObjectArray(Object array) {
        if (array == null) {
            return null;
        }
        if (array instanceof Object[]) {
            return (Object[]) array;
        }
        if (!array.getClass().isArray()) {
            throw new IllegalArgumentException("Not an array: " + array.getClass().getName());
        }

        if (array instanceof byte[]) {
            byte[] src = (byte[]) array;
            Byte[] dst = new Byte[src.length];
            for (int i = 0; i < src.length; i++) {
                dst[i] = src[i];
            }
            return dst;
        } else if (array instanceof short[]) {
            short[] src = (short[]) array;
            Short[] dst = new Short[src.length];
            for (int i = 0; i < src.length; i++) {
                dst[i] = src[i];
            }
            return dst;
        } else if (array instanceof int[]) {
            int[] src = (int[]) array;
            Integer[] dst = new Integer[src.length];
            for (int i = 0; i < src.length; i++) {
                dst[i] = src[i];
            }
            return dst;
        } else if (array instanceof long[]) {
            long[] src = (long[]) array;
            Long[] dst = new Long[src.length];
            for (int i = 0; i < src.length; i++) {
                dst[i] = src[i];
            }
            return dst;
        } else if (array instanceof float[]) {
            float[] src = (float[]) array;
            Float[] dst = new Float[src.length];
            for (int i = 0; i < src.length; i++) {
                dst[i] = src[i];
            }
            return dst;
        } else if (array instanceof double[]) {
            double[] src = (double[]) array;
            Double[] dst = new Double[src.length];
            for (int i = 0; i < src.length; i++) {
                dst[i] = src[i];
            }
            return dst;
        } else if (array instanceof char[]) {
            char[] src = (char[]) array;
            Character[] dst = new Character[src.length];
            for (int i = 0; i < src.length; i++) {
                dst[i] = src[i];
            }
            return dst;
        } else if (array instanceof boolean[]) {
            boolean[] src = (boolean[]) array;
            Boolean[] dst = new Boolean[src.length];
            for (int i = 0; i < src.length; i++) {
                dst[i] = src[i];
            }
            return dst;
        }
        throw new IllegalArgumentException("Cannot convert " + array.getClass().getName() + " to an Object[]");
    }

    public static final String EMPTY_ARRAY_EXPR = "[]";
    public static final String EMPTY_MAP_EXPR = "{}";
    public static final String EMPTY_STRING_EXPR = "''";
    public static final String EMPTY_TUPLE_EXPR = "()";

    private static final byte[] UNHEX_PREFIX = "unhex('".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] UNHEX_SUFFIX = "')".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HEX_ARRAY = "0123456789ABCDEF".getBytes(StandardCharsets.US_ASCII);


    /**
     * Converts given byte array to unhex() expression.
     *
     * @param bytes byte array
     * @return non-null expression
     */
    public static String convertToUnhexExpression(byte[] bytes) {
        int len = bytes != null ? bytes.length : 0;
        if (len == 0) {
            return EMPTY_STRING_EXPR;
        }

        int offset = UNHEX_PREFIX.length;
        byte[] hexChars = new byte[len * 2 + offset + UNHEX_SUFFIX.length];
        System.arraycopy(UNHEX_PREFIX, 0, hexChars, 0, offset);
        System.arraycopy(UNHEX_SUFFIX, 0, hexChars, hexChars.length - UNHEX_SUFFIX.length, UNHEX_SUFFIX.length);
        for (int i = 0; i < len; i++) {
            int v = bytes[i] & 0xFF;
            hexChars[offset++] = HEX_ARRAY[v >>> 4];
            hexChars[offset++] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars, StandardCharsets.US_ASCII);
    }

    /**
     * Decodes a hex string into a byte array.
     * Each pair of characters in the input is interpreted as a hexadecimal byte value.
     *
     * @param hexString hex-encoded string (must have even length)
     * @return decoded byte array, or empty array if input is null or empty
     * @throws IllegalArgumentException if the string has odd length or contains non-hex characters
     */
    public static byte[] decodeHexString(String hexString) {
        if (hexString == null || hexString.isEmpty()) {
            return new byte[0];
        }
        int len = hexString.length();
        if (len % 2 != 0) {
            throw new IllegalArgumentException("Hex string must have even length, got " + len);
        }
        byte[] result = new byte[len / 2];
        for (int i = 0; i < result.length; i++) {
            int hi = Character.digit(hexString.charAt(i * 2), 16);
            int lo = Character.digit(hexString.charAt(i * 2 + 1), 16);
            if (hi == -1 || lo == -1) {
                throw new IllegalArgumentException("Invalid hex character at index " + (hi == -1 ? i * 2 : i * 2 + 1));
            }
            result[i] = (byte) ((hi << 4) | lo);
        }
        return result;
    }
}

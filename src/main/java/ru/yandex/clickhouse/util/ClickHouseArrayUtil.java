package ru.yandex.clickhouse.util;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.TimeZone;

import com.google.common.primitives.Primitives;

import ru.yandex.clickhouse.ClickHouseUtil;
import ru.yandex.clickhouse.response.ArrayByteFragment;
import ru.yandex.clickhouse.response.ByteFragment;
import ru.yandex.clickhouse.response.ClickHouseColumnInfo;
import ru.yandex.clickhouse.response.parser.ClickHouseValueParser;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 */
public class ClickHouseArrayUtil {

    private static final char ARRAY_ELEMENTS_SEPARATOR = ',';
    private static final char STRING_QUOTATION = '\'';
    private static final int MAX_ARRAY_DEPTH = 32;

    private ClickHouseArrayUtil() {
        // NOP
    }

    /**
     * @param object
     *            the object to convert to ClickHouse-string representation
     * @param dateTimeZone
     *            TimeZone to use when formatting date values
     * @param dateTimeTimeZone
     *            TimeZone to use when formatting datetime values
     * @return string representation of an object
     */
    public static String arrayToString(Object object, TimeZone dateTimeZone,
        TimeZone dateTimeTimeZone)
    {
        if (!object.getClass().isArray()) {
            throw new IllegalArgumentException("Object must be array");
        }
        if (object.getClass().getComponentType().isPrimitive()) {
            return primitiveArrayToString(object);
        }
        return toString((Object[]) object, dateTimeZone, dateTimeTimeZone);
    }


    public static String toString(Object[] values, TimeZone dateTimeZone, TimeZone dateTimeTimeZone) {
        if (values.length > 0 && values[0] != null && (values[0].getClass().isArray() || values[0] instanceof Collection)) {
            // quote is false to avoid escaping inner '['
            ArrayBuilder builder = new ArrayBuilder(false, dateTimeZone, dateTimeTimeZone);
            for (Object value : values) {
                if (value instanceof Collection) {
                    Object[] objects = ((Collection<?>) value).toArray();
                    builder.append(toString(objects, dateTimeZone, dateTimeTimeZone));
                } else {
                    builder.append(arrayToString(value, dateTimeZone, dateTimeTimeZone));
                }
            }
            return builder.build();
        }
        ArrayBuilder builder = new ArrayBuilder(needQuote(values), dateTimeZone, dateTimeTimeZone);
        for (Object value : values) {
            builder.append(value);
        }
        return builder.build();
    }

    public static String toString(Collection<?> collection, TimeZone dateTimeZone,
        TimeZone dateTimeTimeZone)
    {
        return toString(collection.toArray(), dateTimeZone, dateTimeTimeZone);
    }

    public static Object parseArray(ByteFragment value, boolean useObjects,
        TimeZone timeZone, int arrayLevel, Class<?> elementClass, ClickHouseColumnInfo columnInfo) throws SQLException
    {
        if (arrayLevel > ClickHouseArrayUtil.MAX_ARRAY_DEPTH) {
            throw new IllegalArgumentException("Maximum parse depth exceeded");
        }

        if (value.isNull()) {
            return null;
        }

        if (value.charAt(0) != '[' || value.charAt(value.length() - 1) != ']') {
            throw new IllegalArgumentException("not an array: " + value);
        }

        if ((elementClass == Date.class || elementClass == Timestamp.class) && timeZone == null) {
            throw new IllegalArgumentException("Time zone must be provided for date/dateTime array");
        }

        ByteFragment trim = value.subseq(1, value.length() - 2);

        int index = 0;
        Object array;
        if (arrayLevel > 1) {
            int[] dimensions = new int[arrayLevel];
            dimensions[0] = ClickHouseArrayUtil.getArrayLength(trim);
            array = java.lang.reflect.Array.newInstance(
                useObjects ? elementClass : Primitives.unwrap(elementClass),
                dimensions
            );
        } else {
            array = java.lang.reflect.Array.newInstance(
                useObjects ? elementClass : Primitives.unwrap(elementClass),
                ClickHouseArrayUtil.getArrayLength(trim)
            );
        }
        int fieldStart = 0;
        int currentLevel = 0;
        boolean inQuotation = false;
        for (int chIdx = 0; chIdx < trim.length(); chIdx++) {
            int ch = trim.charAt(chIdx);

            if (ch == '\\') {
                chIdx++;
            }
            inQuotation = ch == STRING_QUOTATION ^ inQuotation;
            if (!inQuotation) {
                if (ch == '[') {
                    currentLevel++;
                } else if (ch == ']') {
                    currentLevel--;
                }
            }

            if (!inQuotation && ch == ARRAY_ELEMENTS_SEPARATOR && currentLevel == 0 || chIdx == trim.length() - 1) {
                int fieldEnd = chIdx == trim.length() - 1 ? chIdx + 1 : chIdx;
                if (trim.charAt(fieldStart) == '\'') {
                    fieldStart++;
                    fieldEnd--;
                }
                ArrayByteFragment fragment = ArrayByteFragment.wrap(trim.subseq(fieldStart, fieldEnd - fieldStart));
                if (arrayLevel > 1) {
                    Object arrayValue = parseArray(
                        fragment, useObjects, timeZone, arrayLevel - 1, elementClass, columnInfo);
                    java.lang.reflect.Array.set(array, index++, arrayValue);
                } else {
                    Object o = useObjects
                        ? ClickHouseValueParser.getParser(elementClass).parse(
                            fragment, columnInfo, timeZone)
                        : ClickHouseValueParser.getParser(elementClass).parseWithDefault(
                            fragment, columnInfo, timeZone);
                    java.lang.reflect.Array.set(array, index++, o);
                }
                fieldStart = chIdx + 1;
            }
        }

        return array;
    }

    public static int getArrayLength(ByteFragment value) {
        if (value.length() == 0) {
            return 0;
        }

        int length = 1;
        boolean inQuotation = false;
        int arrayLevel = 0;
        for (int i = 0; i < value.length(); i++) {
            int ch = value.charAt(i);

            if (ch == '\\') {
                i++;
            }

            inQuotation = ch == ClickHouseArrayUtil.STRING_QUOTATION ^ inQuotation;
            if (!inQuotation) {
                if (ch == '[') {
                    ++arrayLevel;
                } else if (ch == ']') {
                    --arrayLevel;
                } else if (ch == ClickHouseArrayUtil.ARRAY_ELEMENTS_SEPARATOR && arrayLevel == 0) {
                    ++length;
                }
            }
        }
        return length;
    }

    public static Object parseArray(ByteFragment value,
        boolean useObjects, TimeZone timeZone, ClickHouseColumnInfo columnInfo)
        throws SQLException
    {
        return parseArray(value, useObjects, timeZone, columnInfo.getArrayLevel(),
            columnInfo.getEffectiveClickHouseDataType().getJavaClass(), columnInfo);
    }

    static Object parseArray(ByteFragment value, Class<?> clazz,
        boolean useObjects, ClickHouseColumnInfo columnInfo)
        throws SQLException
    {
        return parseArray(value, useObjects, null, columnInfo.getArrayLevel(),
            clazz, columnInfo);
    }

    /**
     * @deprecated convenience for unit tests
     */
    @Deprecated
    static String toString(Collection<?> collection) {
        TimeZone tz = TimeZone.getDefault();
        return toString(collection, tz, tz);
    }

    /**
     * @deprecated convenience for unit tests
     */
    @Deprecated
    static String toString(Object[] values) {
        TimeZone tz = TimeZone.getDefault();
        return toString(values, tz, tz);
    }

    /**
     * @deprecated convenience for unit tests
     */
    @Deprecated
    static String arrayToString(Object object) {
        TimeZone tz = TimeZone.getDefault();
        return arrayToString(object, tz, tz);
    }

    private static String primitiveArrayToString(Object array) {
        if (array instanceof int[]) {
            return toString((int[]) array);
        } else if (array instanceof long[]) {
            return toString((long[]) array);
        } else if (array instanceof float[]) {
            return toString((float[]) array);
        } else if (array instanceof double[]) {
            return toString((double[]) array);
        } else if (array instanceof char[]) {
            return toString((char[]) array);
        } else if (array instanceof byte[]) {
            return toString((byte[]) array);
        } else if (array instanceof short[]) {
            return toString((short[]) array);
        } else {
            throw new IllegalArgumentException("Wrong primitive type: " + array.getClass().getComponentType());
        }
    }

    private static String toString(int[] values) {
        ArrayBuilder builder = new ArrayBuilder(false);
        for (int value : values) {
            builder.append(value);
        }
        return builder.build();
    }

    private static String toString(long[] values) {
        ArrayBuilder builder = new ArrayBuilder(false);
        for (long value : values) {
            builder.append(value);
        }
        return builder.build();
    }

    private static String toString(float[] values) {
        ArrayBuilder builder = new ArrayBuilder(false);
        for (float value : values) {
            builder.append(value);
        }
        return builder.build();
    }

    private static String toString(double[] values) {
        ArrayBuilder builder = new ArrayBuilder(false);
        for (double value : values) {
            builder.append(value);
        }
        return builder.build();
    }

    private static String toString(byte[] values) {
        return "'" + ClickHouseValueFormatter.formatBytes(values) + "'";
    }

    private static String toString(short[] values) {
        ArrayBuilder builder = new ArrayBuilder(false);
        for (short value : values) {
            builder.append(value);
        }
        return builder.build();
    }

    private static String toString(char[] values) {
        ArrayBuilder builder = new ArrayBuilder(true);
        for (char value : values) {
            builder.append(value);
        }
        return builder.build();
    }

    private static boolean needQuote(Object[] objects) {
        Object o = null;
        for (Object u : objects) {
            if (u != null) {
                o = u;
                break;
            }
        }
        return objects.length == 0 || ClickHouseValueFormatter.needsQuoting(o);
    }

    private static class ArrayBuilder {

        private final StringBuilder builder;
        private final boolean quote;
        private final TimeZone dateTimeZone;
        private final TimeZone dateTimeTimeZone;
        private int size = 0;
        private boolean built = false;

        private ArrayBuilder(boolean quote) {
            this(quote, TimeZone.getDefault(), TimeZone.getDefault());
        }

        private ArrayBuilder(boolean quote, TimeZone dateTimeZone,
            TimeZone dateTimeTimeZone)
        {
            this.quote = quote;
            this.builder = new StringBuilder("[");
            this.dateTimeZone = dateTimeZone;
            this.dateTimeTimeZone = dateTimeTimeZone;
        }

        private ArrayBuilder append(Object value) {
            if (built) {
                throw new IllegalStateException("Already built");
            }
            if (size > 0) {
                builder.append(',');
            }
            if (value != null) {
                if (quote) {
                    builder.append('\'');
                }
                if (value instanceof String) {
                    builder.append(quote ? ClickHouseUtil.escape((String) value) : value);
                } else {
                    builder.append(ClickHouseValueFormatter.formatObject(
                        value, dateTimeZone, dateTimeTimeZone));
                }
                if (quote) {
                    builder.append('\'');
                }
            } else {
                builder.append("NULL");
            }
            size++;
            return this;
        }

        private String build() {
            if (!built) {
                builder.append(']');
                built = true;
            }
            return builder.toString();
        }
    }


}

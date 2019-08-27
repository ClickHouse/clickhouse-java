package ru.yandex.clickhouse.util;

import java.util.Collection;
import java.util.TimeZone;

import ru.yandex.clickhouse.ClickHouseUtil;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 */
public class ClickHouseArrayUtil {

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

package ru.yandex.clickhouse.util;

import ru.yandex.clickhouse.ClickHouseUtil;

import java.util.Collection;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 */
public class ClickHouseArrayUtil {
    private ClickHouseArrayUtil() {
    }

    public static String arrayToString(Object object) {
        if (!object.getClass().isArray()) {
            throw new IllegalArgumentException("Object must be array");
        }
        if (object.getClass().getComponentType().isPrimitive()) {
            return primitiveArrayToString(object);
        } else {
            return toString((Object[]) object);
        }
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

    public static String toString(int[] values) {
        ArrayBuilder builder = new ArrayBuilder(false);
        for (int value : values) {
            builder.append(value);
        }
        return builder.build();
    }

    public static String toString(long[] values) {
        ArrayBuilder builder = new ArrayBuilder(false);
        for (long value : values) {
            builder.append(value);
        }
        return builder.build();
    }

    public static String toString(float[] values) {
        ArrayBuilder builder = new ArrayBuilder(false);
        for (float value : values) {
            builder.append(value);
        }
        return builder.build();
    }

    public static String toString(double[] values) {
        ArrayBuilder builder = new ArrayBuilder(false);
        for (double value : values) {
            builder.append(value);
        }
        return builder.build();
    }

    public static String toString(byte[] values) {
        ArrayBuilder builder = new ArrayBuilder(false);
        for (byte value : values) {
            builder.append(value);
        }
        return builder.build();
    }

    public static String toString(short[] values) {
        ArrayBuilder builder = new ArrayBuilder(false);
        for (short value : values) {
            builder.append(value);
        }
        return builder.build();
    }


    public static String toString(char[] values) {
        ArrayBuilder builder = new ArrayBuilder(true);
        for (char value : values) {
            builder.append(value);
        }
        return builder.build();
    }


    public static String toString(Object[] values) {
        if (values.length > 0 && (values[0].getClass().isArray() || values[0] instanceof Collection)) {
            // quote is false to avoid escaping inner '['
            ArrayBuilder builder = new ArrayBuilder(false);
            for (Object value : values) {
                if (value instanceof Collection) {
                    Object[] objects = ((Collection) value).toArray();
                    builder.append(toString(objects));
                } else {
                    builder.append(arrayToString(value));
                }
            }
            return builder.build();
        }
        ArrayBuilder builder = new ArrayBuilder(needQuote(values));
        for (Object value : values) {
            builder.append(value);
        }
        return builder.build();
    }

    public static String toString(Collection collection) {
        return toString(collection.toArray());
    }

    private static boolean needQuote(Object[] objects) {
        return objects.length == 0 || !(objects[0] instanceof Number);
    }

    private static class ArrayBuilder {
        private final StringBuilder builder = new StringBuilder();
        private final boolean quote;
        private int size = 0;
        private boolean built = false;

        private ArrayBuilder(boolean quote) {
            this.quote = quote;
            builder.append('[');
        }

        private ArrayBuilder append(Object value) {
            String serializedValue = value.toString();
            if (quote) {
                serializedValue = ClickHouseUtil.escape(serializedValue);
            }

            if (built) {
                throw new IllegalStateException("Already built");
            }
            if (size > 0) {
                builder.append(',');
            }
            if (quote) {
                builder.append('\'');
            }
            builder.append(serializedValue);
            if (quote) {
                builder.append('\'');
            }
            size++;
            return this;
        }

        private String build() {
            if (!built) {
                builder.append(']');
                built = false;
            }
            return builder.toString();
        }
    }
}

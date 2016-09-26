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
            builder.append(Integer.toString(value));
        }
        return builder.build();
    }

    public static String toString(long[] values) {
        ArrayBuilder builder = new ArrayBuilder(false);
        for (long value : values) {
            builder.append(Long.toString(value));
        }
        return builder.build();
    }

    public static String toString(float[] values) {
        ArrayBuilder builder = new ArrayBuilder(false);
        for (float value : values) {
            builder.append(Float.toString(value));
        }
        return builder.build();
    }

    public static String toString(double[] values) {
        ArrayBuilder builder = new ArrayBuilder(false);
        for (double value : values) {
            builder.append(Double.toString(value));
        }
        return builder.build();
    }

    public static String toString(byte[] values) {
        ArrayBuilder builder = new ArrayBuilder(false);
        for (byte value : values) {
            builder.append(Byte.toString(value));
        }
        return builder.build();
    }

    public static String toString(short[] values) {
        ArrayBuilder builder = new ArrayBuilder(false);
        for (short value : values) {
            builder.append(Short.toString(value));
        }
        return builder.build();
    }


    public static String toString(char[] values) {
        ArrayBuilder builder = new ArrayBuilder(true);
        for (char value : values) {
            builder.append(Character.toString(value));
        }
        return builder.build();
    }


    public static String toString(Object[] values) {
        ArrayBuilder builder = new ArrayBuilder(needQuote(values));
        for (Object value : values) {
            builder.append(value.toString());
        }
        return builder.build();
    }

    public static String toString(Collection collection) {
        return toString(collection.toArray());
    }

    private static boolean needQuote(Object[] objects) {
        return !Number.class.isAssignableFrom(objects.getClass().getComponentType());
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

        private ArrayBuilder append(String value) {
            if (quote) {
                value = ClickHouseUtil.escape(value);
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
            builder.append(value);
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

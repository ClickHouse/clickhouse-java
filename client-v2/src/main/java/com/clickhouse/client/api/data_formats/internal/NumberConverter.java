package com.clickhouse.client.api.data_formats.internal;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class NumberConverter {

    public static final Map<NumberType, Function<Object, ?>> NUMBER_CONVERTERS = getNumberConverters();

    public enum NumberType {
        Byte("byte"), Short("short"), Int("int"), Long("long"), BigInteger("BigInteger"), Float("float"),
        Double("double"), BigDecimal("BigDecimal"), Boolean("boolean");

        private final String typeName;

        NumberType(String typeName) {
            this.typeName = typeName;
        }

        public String getTypeName() {
            return typeName;
        }
    }


    public static byte toByte(Object value) {
        if (value instanceof Number) {
            Number number = (Number) value;
            if (number.byteValue() == number.shortValue()) {
                return number.byteValue();
            } else {
                throw new ArithmeticException("integer overflow: " + value + " cannot be presented as byte");
            }
        } else if (value instanceof Boolean) {
            return (byte) ((Boolean) value ? 1 : 0);
        } else if (value instanceof String) {
            return Byte.parseByte(value.toString());
        } else {
            throw new IllegalArgumentException("Cannot convert " + value + " to byte value");
        }
    }

    public static short toShort(Object value) {
        if (value instanceof Number) {
            Number number = (Number) value;
            if (number.shortValue() == number.intValue()) {
                return number.shortValue();
            } else {
                throw new ArithmeticException("integer overflow: " + value + " cannot be presented as short");
            }
        } else if (value instanceof Boolean) {
            return (short) ((Boolean) value ? 1 : 0);
        } else if (value instanceof String) {
            return Short.parseShort(value.toString());
        } else {
            throw new IllegalArgumentException("Cannot convert " + value + " to short value");
        }
    }

    public static int toInt(Object value) {
        if (value instanceof Number) {
            Number number = (Number) value;
            if (number.intValue() == number.longValue()) {
                return number.intValue();
            } else {
                throw new ArithmeticException("integer overflow: " + value + " cannot be presented as int");
            }
        } else if (value instanceof Boolean) {
            return (Boolean) value ? 1 : 0;
        } else if (value instanceof String) {
            return Integer.parseInt((String) value);
        } else {
            throw new IllegalArgumentException("Cannot convert " + value + " to int value");
        }
    }

    public static long toLong(Object value) {
        if (value instanceof Number) {
            Number number = (Number) value;
            if (number.longValue() == number.doubleValue()) {
                return number.longValue();
            } else {
                throw new ArithmeticException("integer overflow: " + value + " cannot be presented as long");
            }
        } else if (value instanceof Boolean) {
            return (Boolean) value ? 1 : 0;
        } else if (value instanceof String) {
            return Long.parseLong((String) value);
        } else {
            throw new IllegalArgumentException("Cannot convert " + value + " to long value");
        }
    }

    public static BigInteger toBigInteger(Object value) {
        if (value instanceof BigInteger) {
            return (BigInteger) value;
        } else if (value instanceof Number) {
            return BigInteger.valueOf(((Number) value).longValue());
        } else if (value instanceof Boolean) {
            return (Boolean) value ? BigInteger.ONE : BigInteger.ZERO;
        } else if (value instanceof String) {
            return new BigInteger((String) value);
        } else {
            throw new IllegalArgumentException("Cannot convert " + value + " to BigInteger value");
        }
    }

    public static float toFloat(Object value) {
        if (value instanceof Float) {
            return (Float) value;
        } else if (value instanceof Number) {
            return ((Number) value).floatValue();
        } else if (value instanceof Boolean) {
            return (Boolean) value ? 1.0f : 0.0f;
        } else if (value instanceof String) {
            return Float.parseFloat((String) value);
        } else {
            throw new IllegalArgumentException("Cannot convert " + value + " to float value");
        }
    }

    public static double toDouble(Object value) {
        if (value instanceof Double) {
            return (Double) value;
        } else if (value instanceof Number) {
            return ((Number) value).doubleValue();
        } else if (value instanceof Boolean) {
            return (Boolean) value ? 1.0 : 0.0;
        } else if (value instanceof String) {
            return Double.parseDouble((String) value);
        } else {
            throw new IllegalArgumentException("Cannot convert " + value + " to double value");
        }
    }

    public static BigDecimal toBigDecimal(Object value) {
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        } else if (value instanceof BigInteger) {
            return new BigDecimal((BigInteger) value);
        } else if (value instanceof Number) {
            return BigDecimal.valueOf(((Number) value).doubleValue());
        } else if (value instanceof String) {
            return new BigDecimal((String) value);
        } else if (value instanceof Boolean) {
            return (Boolean) value ? BigDecimal.ONE : BigDecimal.ZERO;
        } else {
            throw new IllegalArgumentException("Cannot convert " + value + " to BigDecimal value");
        }
    }

    static Map<NumberType, Function<Object, ?>> getNumberConverters() {
        Map<NumberType, Function<Object, ?>> converters = new HashMap<>();
        converters.put(NumberType.Byte, NumberConverter::toByte);
        converters.put(NumberType.Short, NumberConverter::toShort);
        converters.put(NumberType.Int, NumberConverter::toInt);
        converters.put(NumberType.Long, NumberConverter::toLong);
        converters.put(NumberType.BigInteger, NumberConverter::toBigInteger);
        converters.put(NumberType.BigDecimal, NumberConverter::toBigDecimal);
        converters.put(NumberType.Float, NumberConverter::toFloat);
        converters.put(NumberType.Double, NumberConverter::toDouble);
        converters.put(NumberType.Boolean, SerializerUtils::convertToBoolean);
        return Collections.unmodifiableMap(converters);
    }
}

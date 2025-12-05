package com.clickhouse.client.api.data_formats.internal;

import com.google.common.collect.ImmutableMap;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public final class ValueConverters {


    // <source type, <target type, converter>>
    private final Map<Class<?>, Map<Class<?>, Function<Object, Object>>> classConverters;

    // <target type, converter>
    private final ImmutableMap<Class<?>, Function<Object, Object>> numberConverters;
    
    public ValueConverters() {


        ImmutableMap.Builder<Class<?>, Function<Object, Object>> numberConvertersBuilder = ImmutableMap.builder();
        numberConvertersBuilder.put(String.class, this::convertNumberToString);
        numberConvertersBuilder.put(Boolean.class, this::convertNumberToBoolean);
        numberConvertersBuilder.put(byte.class, this::convertNumberToByte);
        numberConvertersBuilder.put(short.class, this::convertNumberToShort);
        numberConvertersBuilder.put(int.class, this::convertNumberToInt);
        numberConvertersBuilder.put(long.class, this::convertNumberToLong);
        numberConvertersBuilder.put(float.class, this::convertNumberToFloat);
        numberConvertersBuilder.put(double.class, this::convertNumberToDouble);
        numberConvertersBuilder.put(Byte.class, this::convertNumberToByte);
        numberConvertersBuilder.put(Short.class, this::convertNumberToShort);
        numberConvertersBuilder.put(Integer.class, this::convertNumberToInt);
        numberConvertersBuilder.put(Long.class, this::convertNumberToLong);
        numberConvertersBuilder.put(Float.class, this::convertNumberToFloat);
        numberConvertersBuilder.put(Double.class, this::convertNumberToDouble);
        numberConvertersBuilder.put(BigInteger.class, this::convertNumberToBigInteger);
        numberConvertersBuilder.put(BigDecimal.class, this::convertNumberToBigDecimal);
        
        numberConverters = numberConvertersBuilder.build();

        
        ImmutableMap.Builder<Class<?>, Map<Class<?>, Function<Object, Object>>> mapBuilder = ImmutableMap.builder();
        
        mapBuilder.put(byte.class, numberConverters);
        mapBuilder.put(short.class, numberConverters);
        mapBuilder.put(int.class, numberConverters);
        mapBuilder.put(long.class, numberConverters);
        mapBuilder.put(float.class, numberConverters);
        mapBuilder.put(double.class, numberConverters);
        mapBuilder.put(Byte.class, numberConverters);
        mapBuilder.put(Short.class, numberConverters);
        mapBuilder.put(Integer.class, numberConverters);
        mapBuilder.put(Long.class, numberConverters);
        mapBuilder.put(Float.class, numberConverters);
        mapBuilder.put(Double.class, numberConverters);
        mapBuilder.put(BigInteger.class, numberConverters);
        mapBuilder.put(BigDecimal.class, numberConverters);

        ImmutableMap.Builder<Class<?>, Function<Object, Object>> booleanMapBuilder = ImmutableMap.builder();
        booleanMapBuilder.put(byte.class, this::convertBooleanToNumber);
        booleanMapBuilder.put(short.class, this::convertBooleanToNumber);
        booleanMapBuilder.put(int.class, this::convertBooleanToNumber);
        booleanMapBuilder.put(long.class, this::convertBooleanToNumber);
        booleanMapBuilder.put(float.class, this::convertBooleanToNumber);
        booleanMapBuilder.put(double.class, this::convertBooleanToNumber);
        booleanMapBuilder.put(Byte.class, this::convertBooleanToNumber);
        booleanMapBuilder.put(Short.class, this::convertBooleanToNumber);
        booleanMapBuilder.put(Integer.class, this::convertBooleanToNumber);
        booleanMapBuilder.put(Long.class, this::convertBooleanToNumber);
        booleanMapBuilder.put(Float.class, this::convertBooleanToNumber);
        booleanMapBuilder.put(Double.class, this::convertBooleanToNumber);
        booleanMapBuilder.put(BigInteger.class, this::convertBooleanToNumber);
        booleanMapBuilder.put(BigDecimal.class, this::convertBooleanToNumber);
        booleanMapBuilder.put(String.class, this::convertBooleanToString);
        booleanMapBuilder.put(Boolean.class, this::convertBooleanToBoolean);
        booleanMapBuilder.put(boolean.class, this::convertBooleanToBoolean);

        mapBuilder.put(Boolean.class, booleanMapBuilder.build());
        mapBuilder.put(boolean.class, booleanMapBuilder.build());

        ImmutableMap.Builder<Class<?>, Function<Object, Object>> stringMapBuilder = ImmutableMap.builder();
        stringMapBuilder.put(byte.class, this::convertStringToByte);
        stringMapBuilder.put(short.class, this::convertStringToShort);
        stringMapBuilder.put(int.class, this::convertStringToInt);
        stringMapBuilder.put(long.class, this::convertStringToLong);
        stringMapBuilder.put(float.class, this::convertStringToFloat);
        stringMapBuilder.put(double.class, this::convertStringToDouble);
        stringMapBuilder.put(Byte.class, this::convertStringToByte);
        stringMapBuilder.put(Short.class, this::convertStringToShort);
        stringMapBuilder.put(Integer.class, this::convertStringToInt);
        stringMapBuilder.put(Long.class, this::convertStringToLong);
        stringMapBuilder.put(Float.class, this::convertStringToFloat);
        stringMapBuilder.put(Double.class, this::convertStringToDouble);
        stringMapBuilder.put(Boolean.class, this::convertStringToBoolean);
        stringMapBuilder.put(String.class, this::convertStringToString);
        stringMapBuilder.put(byte[].class, this::convertStringToBytes);
        mapBuilder.put(String.class, stringMapBuilder.build());

        classConverters = mapBuilder.build();
    }

    // Boolean to any
    public Boolean convertBooleanToBoolean(Object value) {
        return (Boolean) value;
    }

    public Number convertBooleanToNumber(Object value) {
        return ((Number) (((Boolean)value) ? 1 : 0)).longValue();
    }

    public String convertBooleanToString(Object value) {
        return String.valueOf(value);
    }

    // String to any
    public String convertStringToString(Object value) {
        return (String) value;
    }

    public byte[] convertStringToBytes(Object value) {
        return ((String) value).getBytes();
    }

    public boolean convertStringToBoolean(Object value) {
        return Boolean.parseBoolean((String) value);
    }

    public byte convertStringToByte(Object value) {
        return Byte.parseByte((String) value);
    }

    public short convertStringToShort(Object value) {
        return Short.parseShort((String) value);
    }

    public int convertStringToInt(Object value) {
        return Integer.parseInt((String) value);
    }

    public long convertStringToLong(Object value) {
        return Long.parseLong((String) value);
    }

    public float convertStringToFloat(Object value) {
        return Float.parseFloat((String) value);
    }

    public double convertStringToDouble(Object value) {
        return Double.parseDouble((String) value);
    }

    // Number to any
    public String convertNumberToString(Object value) {
        return String.valueOf(value);
    }

    public boolean convertNumberToBoolean(Object value) {
        return ((Number) value).floatValue() != 0.0f;
    }

    public byte convertNumberToByte(Object value) {
        return ((Number) value).byteValue();
    }

    public short convertNumberToShort(Object value) {
        return ((Number) value).shortValue();
    }

    public int convertNumberToInt(Object value) {
        return ((Number) value).intValue();
    }

    public long convertNumberToLong(Object value) {
        return ((Number) value).longValue();
    }

    public float convertNumberToFloat(Object value) {
        return ((Number) value).floatValue();
    }

    public double convertNumberToDouble(Object value) {
        return ((Number) value).doubleValue();
    }

    public BigInteger convertNumberToBigInteger(Object value) {
        return BigInteger.valueOf(((Number) value).longValue());
    }

    public BigDecimal convertNumberToBigDecimal(Object value) {
        return BigDecimal.valueOf(((Number) value).doubleValue());
    }

    /**
     * Returns the converter map for the given source type.
     * Map contains target type and converter function. For example, if source type is boolean then map will contain all
     * converters that support converting boolean to target type.
     * @param type - source type
     * @return - map of target type and converter function
     */
    public Map<Class<?>, Function<Object, Object>> getConvertersForType(Class<?> type) {
        return classConverters.getOrDefault(type, Collections.emptyMap());
    }
}

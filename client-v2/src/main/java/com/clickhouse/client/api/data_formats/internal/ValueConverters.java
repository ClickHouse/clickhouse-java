package com.clickhouse.client.api.data_formats.internal;

import java.math.BigDecimal;
import java.math.BigInteger;

public final class ValueConverters {

    // Boolean to any
    public Number convertBooleanToNumber(Object value) {
        return ((Boolean)value) ? 1 : 0;
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
}

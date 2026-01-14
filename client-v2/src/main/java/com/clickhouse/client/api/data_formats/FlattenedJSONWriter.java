package com.clickhouse.client.api.data_formats;

import com.clickhouse.client.api.data_formats.internal.SerializerUtils;
import com.clickhouse.data.ClickHouseColumn;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;

public class FlattenedJSONWriter {

    private final RowBinaryFormatSerializer serializer;
    private final OutputStream out;

    public FlattenedJSONWriter(RowBinaryFormatSerializer serializer) {
        this.serializer = serializer;
        this.out = serializer.getOutputStream();
    }

    /**
     * Writes a key-value pair where the key is a string and the value is a string.
     * The key is written as a string, and the value is written as a dynamic type (tag + value).
     *
     * @param key the key to write
     * @param value the string value to write
     * @throws IOException if an I/O error occurs
     */
    public void writeString(String key, String value) throws IOException {
        serializer.writeString(key);
        writeDynamicValue(value);
    }

    /**
     * Writes a key-value pair where the key is a string and the value is a byte.
     * The key is written as a string, and the value is written as a dynamic type (tag + value).
     *
     * @param key the key to write
     * @param value the byte value to write
     * @throws IOException if an I/O error occurs
     */
    public void writeByte(String key, byte value) throws IOException {
        serializer.writeString(key);
        writeDynamicValue(value);
    }

    /**
     * Writes a key-value pair where the key is a string and the value is a short.
     * The key is written as a string, and the value is written as a dynamic type (tag + value).
     *
     * @param key the key to write
     * @param value the short value to write
     * @throws IOException if an I/O error occurs
     */
    public void writeShort(String key, short value) throws IOException {
        serializer.writeString(key);
        writeDynamicValue(value);
    }

    /**
     * Writes a key-value pair where the key is a string and the value is an int.
     * The key is written as a string, and the value is written as a dynamic type (tag + value).
     *
     * @param key the key to write
     * @param value the int value to write
     * @throws IOException if an I/O error occurs
     */
    public void writeInt(String key, int value) throws IOException {
        serializer.writeString(key);
        writeDynamicValue(value);
    }

    /**
     * Writes a key-value pair where the key is a string and the value is a long.
     * The key is written as a string, and the value is written as a dynamic type (tag + value).
     *
     * @param key the key to write
     * @param value the long value to write
     * @throws IOException if an I/O error occurs
     */
    public void writeLong(String key, long value) throws IOException {
        serializer.writeString(key);
        writeDynamicValue(value);
    }

    /**
     * Writes a key-value pair where the key is a string and the value is a BigInteger.
     * The key is written as a string, and the value is written as a dynamic type (tag + value).
     *
     * @param key the key to write
     * @param value the BigInteger value to write
     * @throws IOException if an I/O error occurs
     */
    public void writeBigInteger(String key, BigInteger value) throws IOException {
        serializer.writeString(key);
        writeDynamicValue(value);
    }

    /**
     * Writes a key-value pair where the key is a string and the value is a float.
     * The key is written as a string, and the value is written as a dynamic type (tag + value).
     *
     * @param key the key to write
     * @param value the float value to write
     * @throws IOException if an I/O error occurs
     */
    public void writeFloat(String key, float value) throws IOException {
        serializer.writeString(key);
        writeDynamicValue(value);
    }

    /**
     * Writes a key-value pair where the key is a string and the value is a double.
     * The key is written as a string, and the value is written as a dynamic type (tag + value).
     *
     * @param key the key to write
     * @param value the double value to write
     * @throws IOException if an I/O error occurs
     */
    public void writeDouble(String key, double value) throws IOException {
        serializer.writeString(key);
        writeDynamicValue(value);
    }

    /**
     * Writes a key-value pair where the key is a string and the value is a BigDecimal.
     * The key is written as a string, and the value is written as a dynamic type (tag + value).
     *
     * @param key the key to write
     * @param value the BigDecimal value to write
     * @throws IOException if an I/O error occurs
     */
    public void writeBigDecimal(String key, BigDecimal value) throws IOException {
        serializer.writeString(key);
        writeDynamicValue(value);
    }

    /**
     * Writes a key-value pair where the key is a string and the value is a boolean.
     * The key is written as a string, and the value is written as a dynamic type (tag + value).
     *
     * @param key the key to write
     * @param value the boolean value to write
     * @throws IOException if an I/O error occurs
     */
    public void writeBoolean(String key, boolean value) throws IOException {
        serializer.writeString(key);
        writeDynamicValue(value);
    }

    /**
     * Writes a key-value pair where the key is a string and the value is a LocalDate.
     * The key is written as a string, and the value is written as a dynamic type (tag + value).
     *
     * @param key the key to write
     * @param value the LocalDate value to write
     * @throws IOException if an I/O error occurs
     */
    public void writeDate(String key, LocalDate value) throws IOException {
        serializer.writeString(key);
        writeDynamicValue(value);
    }

    /**
     * Writes a key-value pair where the key is a string and the value is a LocalDateTime.
     * The key is written as a string, and the value is written as a dynamic type (tag + value).
     *
     * @param key the key to write
     * @param value the LocalDateTime value to write
     * @throws IOException if an I/O error occurs
     */
    public void writeDateTime(String key, LocalDateTime value) throws IOException {
        serializer.writeString(key);
        writeDynamicValue(value);
    }

    /**
     * Writes a key-value pair where the key is a string and the value is a ZonedDateTime.
     * The key is written as a string, and the value is written as a dynamic type (tag + value).
     *
     * @param key the key to write
     * @param value the ZonedDateTime value to write
     * @throws IOException if an I/O error occurs
     */
    public void writeDateTime(String key, ZonedDateTime value) throws IOException {
        serializer.writeString(key);
        writeDynamicValue(value);
    }

    /**
     * Writes a key-value pair where the key is a string and the value is an Object.
     * The key is written as a string, and the value is written as a dynamic type (tag + value).
     *
     * @param key the key to write
     * @param value the Object value to write
     * @throws IOException if an I/O error occurs
     */
    public void writeValue(String key, Object value) throws IOException {
        serializer.writeString(key);
        writeDynamicValue(value);
    }

    /**
     * Writes a value as a dynamic type (tag + value).
     * This method determines the type from the value, writes the type tag, and then serializes the value.
     *
     * @param value the value to write as a dynamic type
     * @throws IOException if an I/O error occurs
     */
    private void writeDynamicValue(Object value) throws IOException {
        ClickHouseColumn typeColumn = SerializerUtils.valueToColumnForDynamicType(value);
        SerializerUtils.writeDynamicTypeTag(out, typeColumn);
        SerializerUtils.serializeData(out, value, typeColumn);
    }
}

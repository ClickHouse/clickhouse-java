package com.clickhouse.client.api.data_formats;

import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.value.ClickHouseBitmap;
import com.clickhouse.data.value.ClickHouseGeoMultiPolygonValue;
import com.clickhouse.data.value.ClickHouseGeoPointValue;
import com.clickhouse.data.value.ClickHouseGeoPolygonValue;
import com.clickhouse.data.value.ClickHouseGeoRingValue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAmount;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public interface ClickHouseBinaryFormatReader extends AutoCloseable {

    /**
     * Reads a single value from the stream.
     *
     * @param <T>
     * @return
     */
    <T> T readValue(int colIndex);

    /**
     * Reads a row to an array of objects.
     *
     * @param colName
     * @param <T>
     * @return
     */
    <T> T readValue(String colName);

    boolean hasValue(String colName);

    boolean hasValue(int colIndex);

    /**
     * Checks if there are more rows to read.
     *
     * @return
     */
    boolean hasNext();

    /**
     * Moves cursor to the next row. Must be called before reading the first row. Returns reference to
     * an internal record representation. It means that next call to the method will affect value in returned Map.
     * This is done for memory usage optimization.
     * Method is intended to be used only by the client not an application.
     *
     * @return reference to a map filled with column values or null if no more records are available
     */
    Map<String, Object> next();

    /**
     * Reads column with name `colName` as a string.
     *
     * @param colName - column name
     * @return
     */
    String getString(String colName);

    /**
     * Reads column with name `colName` as a byte.
     *
     * @param colName - column name
     * @return
     */
    byte getByte(String colName);

    /**
     * Reads column with name `colName` as a short.
     *
     * @param colName - column name
     * @return
     */
    short getShort(String colName);

    /**
     * Reads column with name `colName` as an integer.
     *
     * @param colName - column name
     * @return
     */
    int getInteger(String colName);

    /**
     * Reads column with name `colName` as a long.
     *
     * @param colName - column name
     * @return
     */
    long getLong(String colName);

    /**
     * Reads column with name `colName` as a float.
     * Warning: this method may lose precision for float values.
     * 
     * @param colName
     * @return
     */
    float getFloat(String colName);

    /**
     * Reads column with name `colName` as a double.
     * Warning: this method may lose precision for double values.
     *
     * @param colName
     * @return
     */
    double getDouble(String colName);

    /**
     * Reads column with name `colName` as a boolean.
     *
     * @param colName
     * @return
     */
    boolean getBoolean(String colName);

    /**
     * Reads column with name `colName` as a BigInteger.
     *
     * @param colName
     * @return
     */
    BigInteger getBigInteger(String colName);

    /**
     * Reads column with name `colName` as a BigDecimal.
     *
     * @param colName
     * @return
     */
    BigDecimal getBigDecimal(String colName);

    /**
     * Returns the value of the specified column as an Instant. Timezone is derived from the column definition.
     * If no timezone is specified in the column definition then UTC will be used.
     * 
     * If column value is Date or Date32 it will return an Instant with time set to 00:00:00.
     * If column value is DateTime or DateTime32 it will return an Instant with the time part.
     *
     * @param colName
     * @return
     */
    Instant getInstant(String colName);

    /**
     * Returns the value of the specified column as a ZonedDateTime. Timezone is derived from the column definition.
     * If no timezone is specified in the column definition then UTC will be used.
     * 
     * If column value is Date or Date32 it will return a ZonedDateTime with time set to 00:00:00.
     * If column value is DateTime or DateTime32 it will return a ZonedDateTime with the time part.
     *
     * @param colName
     * @return
     */
    ZonedDateTime getZonedDateTime(String colName);

    /**
     * Returns the value of the specified column as a Duration.
     * 
     * If a stored value is bigger than Long.MAX_VALUE then exception will be thrown. In such case
     * use asBigInteger() method.
     * 
     * If value of IntervalQuarter then Duration will be in the unit of Months.
     *
     * @param colName
     * @return Duration in the unit of column type.
     */
    Duration getDuration(String colName);


    /**
     * Returns the value of the specified column as an Inet4Address.
     *
     * @param colName
     * @return
     */
    Inet4Address getInet4Address(String colName);

    /**
     * Returns the value of the specified column as an Inet6Address.
     *
     * @param colName
     * @return
     */
    Inet6Address getInet6Address(String colName);

    /**
     * Returns the value of the specified column as a UUID.
     *
     * @param colName
     * @return
     */
    UUID getUUID(String colName);

    /**
     * Returns the value of the specified column as a ClickHouseGeoPointValue.
     *
     * @param colName
     * @return
     */
    ClickHouseGeoPointValue getGeoPoint(String colName);

    /**
     * Returns the value of the specified column as a ClickHouseGeoRingValue.
     *
     * @param colName
     * @return
     */
    ClickHouseGeoRingValue getGeoRing(String colName);

    /**
     * Returns the value of the specified column as a ClickHouseGeoPolygonValue.
     *
     * @param colName
     * @return
     */
    ClickHouseGeoPolygonValue getGeoPolygon(String colName);

    /**
     * Returns the value of the specified column as a ClickHouseGeoMultiPolygonValue.
     *
     * @param colName
     * @return
     */
    ClickHouseGeoMultiPolygonValue getGeoMultiPolygon(String colName);

    /**
     * @see #getList(int)
     * @param colName - column name
     * @return list of values, or {@code null} if the value is null
     */
    <T> List<T> getList(String colName);

    /**
     * @see #getByteArray(int)
     * @param colName - column name
     * @return array of bytes, or {@code null} if the value is null
     */
    byte[] getByteArray(String colName);

    /**
     * @see #getIntArray(int)
     * @param colName - column name
     * @return array of int values, or {@code null} if the value is null
     */
    int[] getIntArray(String colName);

    /**
     * @see #getLongArray(int)
     * @param colName - column name
     * @return array of long values, or {@code null} if the value is null
     */
    long[] getLongArray(String colName);

    /**
     * @see #getFloatArray(int)
     * @param colName - column name
     * @return array of float values, or {@code null} if the value is null
     */
    float[] getFloatArray(String colName);

    /**
     * @see #getDoubleArray(int)
     * @param colName - column name
     * @return array of double values, or {@code null} if the value is null
     */
    double[] getDoubleArray(String colName);

    /**
     * @see #getBooleanArray(int)
     * @param colName - column name
     * @return array of boolean values, or {@code null} if the value is null
     */
    boolean[] getBooleanArray(String colName);

    /**
     * @see #getShortArray(int)
     * @param colName - column name
     * @return array of short values, or {@code null} if the value is null
     */
    short[] getShortArray(String colName);

    /**
     * @see #getStringArray(int)
     * @param colName - column name
     * @return array of string values, or {@code null} if the value is null
     */
    String[] getStringArray(String colName);

    /**
     * @see #getObjectArray(int)
     * @param colName - column name
     * @return array of objects, or {@code null} if the value is null
     */
    Object[] getObjectArray(String colName);

    /**
     * Reads column with name `colName` as a string.
     *
     * @param index
     * @return
     */
    String getString(int index);

    /**
     * Reads column with name `colName` as a byte.
     *
     * @param index
     * @return
     */
    byte getByte(int index);

    /**
     * Reads column with name `colName` as a short.
     *
     * @param index
     * @return
     */
    short getShort(int index);

    /**
     * Reads column with name `colName` as an integer.
     *
     * @param index
     * @return
     */
    int getInteger(int index);

    /**
     * Reads column with name `colName` as a long.
     *
     * @param index
     * @return
     */
    long getLong(int index);

    /**
     * Reads column with name `colName` as a float.
     * Warning: this method may lose precision for float values.
     *
     * @param index
     * @return
     */
    float getFloat(int index);

    /**
     * Reads column with name `colName` as a double.
     * Warning: this method may lose precision for double values.
     *
     * @param index
     * @return
     */
    double getDouble(int index);

    /**
     * Reads column with name `colName` as a boolean.
     *
     * @param index
     * @return
     */
    boolean getBoolean(int index);

    /**
     * Reads column with name `colName` as a BigInteger.
     *
     * @param index
     * @return
     */
    BigInteger getBigInteger(int index);

    /**
     * Reads column with name `colName` as a BigDecimal.
     *
     * @param index
     * @return
     */
    BigDecimal getBigDecimal(int index);

    /**
     * Returns the value of the specified column as an Instant. Timezone is derived from the column definition.
     * If no timezone is specified in the column definition then UTC will be used.
     * 
     * If column value is Date or Date32 it will return an Instant with time set to 00:00:00.
     * If column value is DateTime or DateTime32 it will return an Instant with the time part.
     *
     * @param index
     * @return
     */
    Instant getInstant(int index);

    /**
     * Returns the value of the specified column as a ZonedDateTime. Timezone is derived from the column definition.
     * If no timezone is specified in the column definition then UTC will be used.
     * 
     * If column value is Date or Date32 it will return a ZonedDateTime with time set to 00:00:00.
     * If column value is DateTime or DateTime32 it will return a ZonedDateTime with the time part.
     *
     * @param index
     * @return
     */
    ZonedDateTime getZonedDateTime(int index);

    /**
     * Returns the value of the specified column as a Duration.
     * If a stored value is bigger than Long.MAX_VALUE then exception will be thrown. In such case
     * use asBigInteger() method.
     * If value of IntervalQuarter then Duration will be in the unit of Months.
     *
     * @param index
     * @return Duration in the unit of column type.
     */
    Duration getDuration(int index);


    /**
     * Returns the value of the specified column as an Inet4Address.
     *
     * @param index
     * @return
     */
    Inet4Address getInet4Address(int index);

    /**
     * Returns the value of the specified column as an Inet6Address.
     *
     * @param index
     * @return
     */
    Inet6Address getInet6Address(int index);

    /**
     * Returns the value of the specified column as a UUID.
     *
     * @param index
     * @return
     */
    UUID getUUID(int index);

    /**
     * Returns the value of the specified column as a ClickHouseGeoPointValue.
     *
     * @param index
     * @return
     */
    ClickHouseGeoPointValue getGeoPoint(int index);

    /**
     * Returns the value of the specified column as a ClickHouseGeoRingValue.
     *
     * @param index
     * @return
     */
    ClickHouseGeoRingValue getGeoRing(int index);

    /**
     * Returns the value of the specified column as a ClickHouseGeoPolygonValue.
     *
     * @param index
     * @return
     */
    ClickHouseGeoPolygonValue getGeoPolygon(int index);

    /**
     * Returns the value of the specified column as a ClickHouseGeoMultiPolygonValue.
     *
     * @param index
     * @return
     */
    ClickHouseGeoMultiPolygonValue getGeoMultiPolygon(int index);

    /**
     * Returns the value of the specified column as a {@link List}. Suitable for reading Array columns of any type.
     * <p>For nested arrays (e.g. {@code Array(Array(Int64))}), returns a {@code List<List<Long>>}.
     * For nullable arrays (e.g. {@code Array(Nullable(Int32))}), list elements may be {@code null}.</p>
     *
     * @param index - column index (1-based)
     * @return list of values, or {@code null} if the value is null
     * @throws com.clickhouse.client.api.ClientException if the column is not an array type
     */
    <T> List<T> getList(int index);

    /**
     * Returns the value of the specified column as a {@code byte[]}. Suitable for 1D Array columns only.
     *
     * @param index - column index (1-based)
     * @return array of bytes, or {@code null} if the value is null
     * @throws com.clickhouse.client.api.ClientException if the value cannot be converted to a byte array
     */
    byte[] getByteArray(int index);

    /**
     * Returns the value of the specified column as an {@code int[]}. Suitable for 1D Array columns only.
     *
     * @param index - column index (1-based)
     * @return array of int values, or {@code null} if the value is null
     * @throws com.clickhouse.client.api.ClientException if the value cannot be converted to an int array
     */
    int[] getIntArray(int index);

    /**
     * Returns the value of the specified column as a {@code long[]}. Suitable for 1D Array columns only.
     *
     * @param index - column index (1-based)
     * @return array of long values, or {@code null} if the value is null
     * @throws com.clickhouse.client.api.ClientException if the value cannot be converted to a long array
     */
    long[] getLongArray(int index);

    /**
     * Returns the value of the specified column as a {@code float[]}. Suitable for 1D Array columns only.
     *
     * @param index - column index (1-based)
     * @return array of float values, or {@code null} if the value is null
     * @throws com.clickhouse.client.api.ClientException if the value cannot be converted to a float array
     */
    float[] getFloatArray(int index);

    /**
     * Returns the value of the specified column as a {@code double[]}. Suitable for 1D Array columns only.
     *
     * @param index - column index (1-based)
     * @return array of double values, or {@code null} if the value is null
     * @throws com.clickhouse.client.api.ClientException if the value cannot be converted to a double array
     */
    double[] getDoubleArray(int index);

    /**
     * Returns the value of the specified column as a {@code boolean[]}. Suitable for 1D Array columns only.
     *
     * @param index - column index (1-based)
     * @return array of boolean values, or {@code null} if the value is null
     * @throws com.clickhouse.client.api.ClientException if the value cannot be converted to a boolean array
     */
    boolean[] getBooleanArray(int index);

    /**
     * Returns the value of the specified column as a {@code short[]}. Suitable for 1D Array columns only.
     *
     * @param index - column index (1-based)
     * @return array of short values, or {@code null} if the value is null
     * @throws com.clickhouse.client.api.ClientException if the value cannot be converted to a short array
     */
    short[] getShortArray(int index);

    /**
     * Returns the value of the specified column as a {@code String[]}. Suitable for 1D Array columns only.
     * Cannot be used for none string element types.
     *
     * @param index - column index (1-based)
     * @return array of string values, or {@code null} if the value is null
     * @throws com.clickhouse.client.api.ClientException if the column is not an array type
     */
    String[] getStringArray(int index);

    /**
     * Returns the value of the specified column as an {@code Object[]}. Suitable for multidimensional Array columns.
     * Nested arrays are recursively converted to {@code Object[]}.
     * Note: result is not cached so avoid repetitive calls on same column.
     *
     * @param index - column index (1-based)
     * @return array of objects, or {@code null} if the value is null
     * @throws com.clickhouse.client.api.ClientException if the column is not an array type
     */
    Object[] getObjectArray(int index);

    Object[] getTuple(int index);

    Object[] getTuple(String colName);

    byte getEnum8(String colName);

    byte getEnum8(int index);

    short getEnum16(String colName);

    short getEnum16(int index);

    LocalDate getLocalDate(String colName);

    LocalDate getLocalDate(int index);

    LocalTime getLocalTime(String colName);

    LocalTime getLocalTime(int index);

    LocalDateTime getLocalDateTime(String colName);

    LocalDateTime getLocalDateTime(int index);

    OffsetDateTime getOffsetDateTime(String colName);

    OffsetDateTime getOffsetDateTime(int index);

    TableSchema getSchema();

    ClickHouseBitmap getClickHouseBitmap(String colName);

    ClickHouseBitmap getClickHouseBitmap(int index);

    TemporalAmount getTemporalAmount(int index);

    TemporalAmount getTemporalAmount(String colName);
}

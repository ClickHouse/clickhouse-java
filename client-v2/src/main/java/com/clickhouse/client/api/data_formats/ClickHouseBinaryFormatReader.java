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
import java.time.*;
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
     * Reads column with name `colName` as a string.
     *
     * @param colName - column name
     * @return
     */
    <T> List<T> getList(String colName);

    /**
     * Reads column with name `colName` as a string.
     *
     * @param colName - column name
     * @return
     */
    byte[] getByteArray(String colName);

    /**
     * Reads column with name `colName` as a string.
     *
     * @param colName - column name
     * @return
     */
    int[] getIntArray(String colName);

    /**
     * Reads column with name `colName` as a string.
     *
     * @param colName - column name
     * @return
     */
    long[] getLongArray(String colName);

    /**
     * Reads column with name `colName` as a string.
     *
     * @param colName - column name
     * @return
     */
    float[] getFloatArray(String colName);

    /**
     * Reads column with name `colName` as a string.
     *
     * @param colName - column name
     * @return
     */
    double[] getDoubleArray(String colName);

    /**
     *
     * @param colName
     * @return
     */
    boolean[] getBooleanArray(String colName);

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
     * Reads column with name `colName` as a string.
     *
     * @param index - column name
     * @return
     */
    <T> List<T> getList(int index);

    /**
     * Reads column with name `colName` as a string.
     *
     * @param index - column name
     * @return
     */
    byte[] getByteArray(int index);

    /**
     * Reads column with name `colName` as a string.
     *
     * @param index - column name
     * @return
     */
    int[] getIntArray(int index);

    /**
     * Reads column with name `colName` as a string.
     *
     * @param index - column name
     * @return
     */
    long[] getLongArray(int index);

    /**
     * Reads column with name `colName` as a string.
     *
     * @param index - column name
     * @return
     */
    float[] getFloatArray(int index);

    /**
     * Reads column with name `colName` as a string.
     *
     * @param index - column name
     * @return
     */
    double[] getDoubleArray(int index);

    boolean[] getBooleanArray(int index);

    Object[] getTuple(int index);

    Object[] getTuple(String colName);

    byte getEnum8(String colName);

    byte getEnum8(int index);

    short getEnum16(String colName);

    short getEnum16(int index);

    LocalDate getLocalDate(String colName);

    LocalDate getLocalDate(int index);

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

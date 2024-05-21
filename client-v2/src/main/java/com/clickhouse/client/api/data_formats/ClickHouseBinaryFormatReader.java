package com.clickhouse.client.api.data_formats;

import com.clickhouse.data.value.ClickHouseGeoMultiPolygonValue;
import com.clickhouse.data.value.ClickHouseGeoPointValue;
import com.clickhouse.data.value.ClickHouseGeoPolygonValue;
import com.clickhouse.data.value.ClickHouseGeoRingValue;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public interface ClickHouseBinaryFormatReader {

    /**
     * Reads a single value from the stream.
     *
     * @param <T>
     * @return
     * @throws IOException
     */
    <T> T readValue(int colIndex);

    /**
     * Reads a row to an array of objects.
     *
     * @param colName
     * @param <T>
     * @return
     * @throws IOException
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
     * Moves cursor to the next row. Must be called before reading the first row.
     *
     * @return true if there are more rows to read, false otherwise
     */
    boolean next();

    /**
     * Copies current record to a map.
     *
     * @param destination - map to which the record will be copied
     * @throws IOException
     */
    void copyRecord(Map<String, Object> destination) throws IOException;

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
     *
     * @param colName
     * @return
     */
    float getFloat(String colName);

    /**
     * Reads column with name `colName` as a double.
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
     * </br>
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
     * </br>
     * If column value is Date or Date32 it will return a ZonedDateTime with time set to 00:00:00.
     * If column value is DateTime or DateTime32 it will return a ZonedDateTime with the time part.
     *
     * @param colName
     * @return
     */
    ZonedDateTime getZonedDateTime(String colName);

    /**
     * Returns the value of the specified column as a Duration.
     * </br>
     * If a stored value is bigger than Long.MAX_VALUE then exception will be thrown. In such case
     * use asBigInteger() method.
     * </br>
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
    ClickHouseGeoPolygonValue  asGeoPolygon(String colName);

    /**
     * Returns the value of the specified column as a ClickHouseGeoMultiPolygonValue.
     *
     * @param colName
     * @return
     */
    ClickHouseGeoMultiPolygonValue asGeoMultiPolygon(String colName);

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
    <T> List<List<T>> getTwoDimensionalList(String colName);

    /**
     * Reads column with name `colName` as a string.
     *
     * @param colName - column name
     * @return
     */
    <T> List<List<List<T>>> getThreeDimensionalList(String colName);

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
     * Reads column with name `colName` as a string.
     *
     * @param colName - column name
     * @return
     */
    String getString(int index);

    /**
     * Reads column with name `colName` as a byte.
     *
     * @param colName - column name
     * @return
     */
    byte getByte(int index);

    /**
     * Reads column with name `colName` as a short.
     *
     * @param colName - column name
     * @return
     */
    short getShort(int index);

    /**
     * Reads column with name `colName` as an integer.
     *
     * @param colName - column name
     * @return
     */
    int getInteger(int index);

    /**
     * Reads column with name `colName` as a long.
     *
     * @param colName - column name
     * @return
     */
    long getLong(int index);

    /**
     * Reads column with name `colName` as a float.
     *
     * @param colName
     * @return
     */
    float getFloat(int index);

    /**
     * Reads column with name `colName` as a double.
     *
     * @param colName
     * @return
     */
    double getDouble(int index);

    /**
     * Reads column with name `colName` as a boolean.
     *
     * @param colName
     * @return
     */
    boolean getBoolean(int index);

    /**
     * Reads column with name `colName` as a BigInteger.
     *
     * @param colName
     * @return
     */
    BigInteger getBigInteger(int index);

    /**
     * Reads column with name `colName` as a BigDecimal.
     *
     * @param colName
     * @return
     */
    BigDecimal getBigDecimal(int index);

    /**
     * Returns the value of the specified column as an Instant. Timezone is derived from the column definition.
     * If no timezone is specified in the column definition then UTC will be used.
     * </br>
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
     * </br>
     * If column value is Date or Date32 it will return a ZonedDateTime with time set to 00:00:00.
     * If column value is DateTime or DateTime32 it will return a ZonedDateTime with the time part.
     *
     * @param colName
     * @return
     */
    ZonedDateTime getZonedDateTime(int index);

    /**
     * Returns the value of the specified column as a Duration.
     * </br>
     * If a stored value is bigger than Long.MAX_VALUE then exception will be thrown. In such case
     * use asBigInteger() method.
     * </br>
     * If value of IntervalQuarter then Duration will be in the unit of Months.
     *
     * @param colName
     * @return Duration in the unit of column type.
     */
    Duration getDuration(int index);


    /**
     * Returns the value of the specified column as an Inet4Address.
     *
     * @param colName
     * @return
     */
    Inet4Address getInet4Address(int index);

    /**
     * Returns the value of the specified column as an Inet6Address.
     *
     * @param colName
     * @return
     */
    Inet6Address getInet6Address(int index);

    /**
     * Returns the value of the specified column as a UUID.
     *
     * @param colName
     * @return
     */
    UUID getUUID(int index);

    /**
     * Returns the value of the specified column as a ClickHouseGeoPointValue.
     *
     * @param colName
     * @return
     */
    ClickHouseGeoPointValue getGeoPoint(int index);

    /**
     * Returns the value of the specified column as a ClickHouseGeoRingValue.
     *
     * @param colName
     * @return
     */
    ClickHouseGeoRingValue getGeoRing(int index);

    /**
     * Returns the value of the specified column as a ClickHouseGeoPolygonValue.
     *
     * @param colName
     * @return
     */
    ClickHouseGeoPolygonValue  asGeoPolygon(int index);

    /**
     * Returns the value of the specified column as a ClickHouseGeoMultiPolygonValue.
     *
     * @param colName
     * @return
     */
    ClickHouseGeoMultiPolygonValue asGeoMultiPolygon(int index);

    /**
     * Reads column with name `colName` as a string.
     *
     * @param colName - column name
     * @return
     */
    <T> List<T> getList(int index);

    /**
     * Reads column with name `colName` as a string.
     *
     * @param colName - column name
     * @return
     */
    <T> List<List<T>> getTwoDimensionalList(int index);

    /**
     * Reads column with name `colName` as a string.
     *
     * @param colName - column name
     * @return
     */
    <T> List<List<List<T>>> getThreeDimensionalList(int index);

    /**
     * Reads column with name `colName` as a string.
     *
     * @param colName - column name
     * @return
     */
    byte[] getByteArray(int index);

    /**
     * Reads column with name `colName` as a string.
     *
     * @param colName - column name
     * @return
     */
    int[] getIntArray(int index);

    /**
     * Reads column with name `colName` as a string.
     *
     * @param colName - column name
     * @return
     */
    long[] getLongArray(int index);

    /**
     * Reads column with name `colName` as a string.
     *
     * @param colName - column name
     * @return
     */
    float[] getFloatArray(int index);

    /**
     * Reads column with name `colName` as a string.
     *
     * @param colName - column name
     * @return
     */
    double[] getDoubleArray(int index);

    Object[] getTuple(int index);

    Object[] getTuple(String colName);

    byte getEnum8(String colName);

    byte getEnum8(int index);

    short getEnum16(String colName);

    short getEnum16(int index);
}

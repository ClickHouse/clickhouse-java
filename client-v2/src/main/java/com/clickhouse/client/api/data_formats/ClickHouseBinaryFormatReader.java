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
    <T> T readValue(int colIndex) throws IOException;

    /**
     * Reads a row to an array of objects.
     *
     * @param colName
     * @param <T>
     * @return
     * @throws IOException
     */
    <T> T readValue(String colName);

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

    String getString(String colName);

    Byte getByte(String colName);

    Short getShort(String colName);

    Integer getInteger(String colName);

    Long getLong(String colName);

    Float getFloat(String colName);

    Double getDouble(String colName);

    Boolean getBoolean(String colName);

    BigInteger getBigInteger(String colName);

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


    Inet4Address getInet4Address(String colName);

    Inet6Address getInet6Address(String colName);

    UUID getUUID(String colName);

    ClickHouseGeoPointValue getGeoPoint(String colName);

    ClickHouseGeoRingValue getGeoRing(String colName);

    ClickHouseGeoPolygonValue  asGeoPolygon(String colName);

    ClickHouseGeoMultiPolygonValue asGeoMultiPolygon(String colName);

    <T> List<T> getList(String colName);

    <T> List<List<T>> getTwoDimensionalList(String colName);

    <T> List<List<List<T>>> getThreeDimensionalList(String colName);

    byte[] getByteArray(String colName);

    int[] getIntArray(String colName);

    long[] getLongArray(String colName);

    float[] getFloatArray(String colName);

    double[] getDoubleArray(String colName);
}

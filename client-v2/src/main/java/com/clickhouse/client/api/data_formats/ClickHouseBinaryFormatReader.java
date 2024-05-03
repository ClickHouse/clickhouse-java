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
     * Moves cursor to next row.
     *
     * @return
     */
    boolean next();

    /**
     * Read a record from the stream and write it to the given map.
     *
     * @param destination - map to which the record will be written
     * @throws IOException
     */
    void readRecord(Map<String, Object> destination) throws IOException;

    String asString(String colName);

    Byte asByte(String colName);

    Short asShort(String colName);

    Integer asInteger(String colName);

    Long asLong(String colName);

    Float asFloat(String colName);

    Double asDouble(String colName);

    Boolean asBoolean(String colName);

    BigInteger asBigInteger(String colName);

    BigDecimal asBigDecimal(String colName);

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
    Instant asInstant(String colName);

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
    ZonedDateTime asZonedDateTime(String colName);

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
    Duration asDuration(String colName);


    Inet4Address asInet4Address(String colName);

    Inet6Address asInet6Address(String colName);

    UUID asUUID(String colName);

    ClickHouseGeoPointValue asGeoPoint(String colName);

    ClickHouseGeoRingValue asGeoRing(String colName);

    ClickHouseGeoPolygonValue  asGeoPolygon(String colName);

    ClickHouseGeoMultiPolygonValue asGeoMultiPolygon(String colName);
}

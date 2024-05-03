package com.clickhouse.client.api.data_formats;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Map;

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

    Instant asInstant(String colName);

    ZonedDateTime asZonedDateTime(String colName);
}

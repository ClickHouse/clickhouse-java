package com.clickhouse.client.api.data_formats.stream;

import com.clickhouse.client.api.data_formats.internal.StringValue;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.value.ClickHouseBitmap;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.UUID;

/**
 * One scope of a streaming RowBinary read. The verbs match {@link ValueWriter}.
 * <p>
 * Experimental API. Not thread-safe. The caller owns the input stream.
 */
public interface ValueReader extends AutoCloseable {

    byte readInt8() throws IOException;

    short readInt16() throws IOException;

    int readInt32() throws IOException;

    long readInt64() throws IOException;

    BigInteger readInt128() throws IOException;

    BigInteger readInt256() throws IOException;

    int readUInt8() throws IOException;

    int readUInt16() throws IOException;

    long readUInt32() throws IOException;

    BigInteger readUInt64() throws IOException;

    BigInteger readUInt128() throws IOException;

    BigInteger readUInt256() throws IOException;

    float readFloat32() throws IOException;

    double readFloat64() throws IOException;

    float readBFloat16() throws IOException;

    boolean readBool() throws IOException;

    String readString() throws IOException;

    StringValue readStringValue() throws IOException;

    String readFixedString() throws IOException;

    LocalDate readDate() throws IOException;

    LocalDate readDate32() throws IOException;

    ZonedDateTime readDateTime() throws IOException;

    ZonedDateTime readDateTime64() throws IOException;

    UUID readUUID() throws IOException;

    Inet4Address readIPv4() throws IOException;

    Inet6Address readIPv6() throws IOException;

    BigDecimal readDecimal() throws IOException;

    int readEnum8() throws IOException;

    int readEnum16() throws IOException;

    ClickHouseBitmap readBitmap() throws IOException;

    /**
     * Consumes a SQL NULL and returns true, or returns false and leaves the body to be read.
     * On a non-nullable {@code Variant} or {@code Geometry}, discriminator {@code 0xFF} is null.
     */
    boolean isNull() throws IOException;

    boolean hasNext();

    int size();

    int variantOrdinal();

    ClickHouseColumn runtimeType();

    ValueReader beginArray() throws IOException;

    ValueReader beginTuple() throws IOException;

    ValueReader beginMap() throws IOException;

    ValueReader beginNested() throws IOException;

    ValueReader beginVariant() throws IOException;

    ValueReader beginDynamic() throws IOException;

    ValueReader beginQBit() throws IOException;

    /**
     * Fills {@code dst} with up to {@code len} {@code Int32} elements of the current array.
     *
     * @return number of elements written into {@code dst}
     */
    int readInto(int[] dst, int off, int len) throws IOException;

    /**
     * Skips the current value without building a list, map, or array.
     */
    void skip() throws IOException;

    @Override
    void close() throws IOException;
}

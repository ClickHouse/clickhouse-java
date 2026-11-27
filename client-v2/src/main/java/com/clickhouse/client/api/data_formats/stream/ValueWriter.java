package com.clickhouse.client.api.data_formats.stream;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.value.ClickHouseBitmap;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.UUID;

/**
 * One scope of a streaming RowBinary write: a row, an array, a tuple, a map, a nested row,
 * a variant arm, or a dynamic value.
 * <p>
 * Experimental API. Not thread-safe. The caller owns the output stream.
 */
public interface ValueWriter extends AutoCloseable {

    void writeInt8(byte value) throws IOException;

    void writeInt8(int value) throws IOException;

    void writeInt16(short value) throws IOException;

    void writeInt16(int value) throws IOException;

    void writeInt32(int value) throws IOException;

    void writeInt32(long value) throws IOException;

    void writeInt64(long value) throws IOException;

    void writeInt128(BigInteger value) throws IOException;

    void writeInt256(BigInteger value) throws IOException;

    void writeUInt8(int value) throws IOException;

    void writeUInt16(int value) throws IOException;

    void writeUInt32(long value) throws IOException;

    void writeUInt64(long value) throws IOException;

    void writeUInt64(BigInteger value) throws IOException;

    void writeUInt128(BigInteger value) throws IOException;

    void writeUInt256(BigInteger value) throws IOException;

    void writeFloat32(float value) throws IOException;

    void writeFloat64(double value) throws IOException;

    void writeBFloat16(float value) throws IOException;

    void writeBool(boolean value) throws IOException;

    void writeString(String value) throws IOException;

    void writeString(byte[] value) throws IOException;

    void writeString(ByteBuffer value) throws IOException;

    void writeFixedString(String value) throws IOException;

    void writeFixedString(byte[] value) throws IOException;

    void writeDate(LocalDate value) throws IOException;

    void writeDate32(LocalDate value) throws IOException;

    void writeDateTime(LocalDateTime value) throws IOException;

    void writeDateTime(ZonedDateTime value) throws IOException;

    void writeDateTime(Instant value) throws IOException;

    void writeDateTime64(LocalDateTime value) throws IOException;

    void writeDateTime64(ZonedDateTime value) throws IOException;

    void writeUUID(UUID value) throws IOException;

    void writeIPv4(Inet4Address value) throws IOException;

    void writeIPv6(Inet6Address value) throws IOException;

    void writeDecimal(BigDecimal value) throws IOException;

    void writeEnum8(int value) throws IOException;

    void writeEnum8(String name) throws IOException;

    void writeEnum16(int value) throws IOException;

    void writeEnum16(String name) throws IOException;

    void writeNull() throws IOException;

    void writeDefault() throws IOException;

    void writeBitmap(ClickHouseBitmap value) throws IOException;

    ValueWriter beginArray() throws IOException;

    ValueWriter beginArray(int size) throws IOException;

    ValueWriter beginTuple() throws IOException;

    ValueWriter beginMap() throws IOException;

    ValueWriter beginMap(int pairs) throws IOException;

    ValueWriter beginNested() throws IOException;

    ValueWriter beginNested(int rows) throws IOException;

    ValueWriter beginVariant(int ordinal) throws IOException;

    ValueWriter beginDynamic(ClickHouseColumn runtimeType) throws IOException;

    ValueWriter beginQBit() throws IOException;

    /**
     * Opens a binary {@code JSON} object. The number of leaf paths is written when the scope closes.
     * A nested object opened with {@link #beginObject(String)} contributes its leaf paths to this count.
     */
    ValueWriter beginJson() throws IOException;

    /**
     * Opens a binary {@code JSON} object with a known number of leaf paths. The count is written immediately.
     */
    ValueWriter beginJson(int paths) throws IOException;

    /** Dynamic {@code String} path. A null value is a JSON null. */
    void writeString(String path, String value) throws IOException;

    /** Dynamic {@code Int32} path. */
    void writeInt32(String path, int value) throws IOException;

    /** Dynamic {@code Int64} path. */
    void writeInt64(String path, long value) throws IOException;

    /** Dynamic {@code Float64} path. */
    void writeFloat64(String path, double value) throws IOException;

    /** Dynamic {@code Bool} path. */
    void writeBool(String path, boolean value) throws IOException;

    /** JSON null at {@code path}: the path is present and its dynamic type is {@code Nothing}. */
    void writeNull(String path) throws IOException;

    /**
     * Nested object stored as dotted paths ({@code address.city}) on the enclosing JSON value.
     * The leaf count is not known yet.
     */
    ValueWriter beginObject(String path) throws IOException;

    /**
     * Nested object stored as dotted paths. {@code paths} is the number of leaf paths it contributes.
     */
    ValueWriter beginObject(String path, int paths) throws IOException;

    /** Dynamic {@code Array} path. The element type selects the array type tag. */
    ValueWriter beginArray(String path, ClickHouseColumn elementType) throws IOException;

    /** Dynamic {@code Array} path with a known element count. */
    ValueWriter beginArray(String path, ClickHouseColumn elementType, int size) throws IOException;

    /** Dynamic path whose value is itself a JSON document, for example one element of {@code Array(JSON)}. */
    ValueWriter beginJson(String path) throws IOException;

    /** Dynamic JSON path with a known number of leaf paths inside that document. */
    ValueWriter beginJson(String path, int paths) throws IOException;

    void writeInt8Array(byte[] values) throws IOException;

    void writeInt16Array(short[] values) throws IOException;

    void writeInt32Array(int[] values) throws IOException;

    void writeInt64Array(long[] values) throws IOException;

    void writeFloat32Array(float[] values) throws IOException;

    void writeFloat64Array(double[] values) throws IOException;

    void writeBFloat16Array(float[] values) throws IOException;

    void writeBoolArray(boolean[] values) throws IOException;

    @Override
    void close() throws IOException;
}

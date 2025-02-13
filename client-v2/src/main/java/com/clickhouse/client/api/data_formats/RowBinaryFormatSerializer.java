package com.clickhouse.client.api.data_formats;

import com.clickhouse.client.api.data_formats.internal.SerializerUtils;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.format.BinaryStreamUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * This class is intended to be used for very precise data serializations.
 * It is an auxiliary class to handle only low level write operations.
 * <p>
 * Experimental API
 */
public class RowBinaryFormatSerializer {

    private OutputStream out;

    public RowBinaryFormatSerializer(OutputStream out) {
        this.out = out;
    }

    public void writeNull() throws IOException {
        SerializerUtils.writeNull(out);
    }

    public void writeNotNull() throws IOException {
        SerializerUtils.writeNonNull(out);
    }

    public void writeDefault() throws IOException {
        SerializerUtils.writeBoolean(out, true);
    }

    public void writeInt8(byte value) throws IOException {
        BinaryStreamUtils.writeInt8(out, value);
    }

    public void writeUInt8(short value) throws IOException {
        BinaryStreamUtils.writeUnsignedInt8(out, value);
    }

    public void writeInt16(short value) throws IOException {
        BinaryStreamUtils.writeInt16(out, value);
    }

    public void writeUInt16(int value) throws IOException {
        BinaryStreamUtils.writeUnsignedInt16(out, value);
    }

    public void writeInt32(int value) throws IOException {
        BinaryStreamUtils.writeInt32(out, value);
    }

    public void writeUInt32(long value) throws IOException {
        BinaryStreamUtils.writeUnsignedInt32(out, value);
    }

    public void writeInt64(long value) throws IOException {
        BinaryStreamUtils.writeInt64(out, value);
    }

    public void writeUInt64(BigInteger value) throws IOException {
        BinaryStreamUtils.writeUnsignedInt64(out, value);
    }

    public void writeInt128(BigInteger value) throws IOException {
        BinaryStreamUtils.writeInt128(out, value);
    }

    public void writeUInt128(BigInteger value) throws IOException {
        BinaryStreamUtils.writeUnsignedInt128(out, value);
    }

    public void writeInt256(BigInteger value) throws IOException {
        BinaryStreamUtils.writeInt256(out, value);
    }

    public void writeUInt256(BigInteger value) throws IOException {
        BinaryStreamUtils.writeUnsignedInt128(out, value);
    }

    public void writeBool(boolean value) throws IOException {
        BinaryStreamUtils.writeBoolean(out, value);
    }

    public void writeFloat32(float value) throws IOException {
        BinaryStreamUtils.writeFloat32(out, value);
    }

    public void writeFloat64(double value) throws IOException {
        BinaryStreamUtils.writeFloat64(out, value);
    }

    public void writeDecimal(BigDecimal value, int precision, int scale) throws IOException {
        BinaryStreamUtils.writeDecimal(out, value, precision, scale);
    }

    public void writeDecimal32(BigDecimal value, int scale) throws IOException {
        BinaryStreamUtils.writeDecimal32(out, value, scale);
    }

    public void writeDecimal64(BigDecimal value, int scale) throws IOException {
        BinaryStreamUtils.writeDecimal64(out, value, scale);
    }

    public void writeDecimal128(BigDecimal value, int scale) throws IOException {
        BinaryStreamUtils.writeDecimal128(out, value, scale);
    }

    public void writeDecimal256(BigDecimal value, int scale) throws IOException {
        BinaryStreamUtils.writeDecimal256(out, value, scale);
    }

    public void writeString(String value) throws IOException {
        BinaryStreamUtils.writeString(out, value);
    }

    public void writeFixedString(String value, int len) throws IOException {
        BinaryStreamUtils.writeFixedString(out, value, len);
    }

    public void writeDate(ZonedDateTime value) throws IOException {
        SerializerUtils.writeDate(out, value, ZoneId.of("UTC"));
    }

    public void writeDate32(ZonedDateTime value, ZoneId targetTz) throws IOException {
        SerializerUtils.writeDate32(out, value, targetTz);
    }

    public void writeDateTime(ZonedDateTime value, ZoneId targetTz) throws IOException {
        SerializerUtils.writeDateTime(out, value, targetTz);
    }

    public void writeDateTime64(ZonedDateTime value, int scale, ZoneId targetTz) throws IOException {
        SerializerUtils.writeDateTime64(out, value, scale, targetTz);
    }

    public void writeDateTime32(OffsetDateTime value) throws IOException {
        SerializerUtils.writeDateTime32(out, value, null);
    }

    public void writeDateTime64(OffsetDateTime value, int scale) throws IOException {
        SerializerUtils.writeDateTime64(out, value, scale, null);
    }

    public void writeDateTime32(Instant value) throws IOException {
        SerializerUtils.writeDateTime32(out, value, null);
    }

    public void writeDateTime64(Instant value, int scale) throws IOException {
        SerializerUtils.writeDateTime64(out, value, scale, null);
    }

    public void writeEnum8(byte value) throws IOException {
        BinaryStreamUtils.writeEnum8(out, value);
    }

    public void writeEnum16(short value) throws IOException {
        BinaryStreamUtils.writeEnum16(out, value);
    }

    public void writeUUID(long leastSignificantBits, long mostSignificantBits) throws IOException {
        BinaryStreamUtils.writeInt64(out, mostSignificantBits);
        BinaryStreamUtils.writeInt64(out, leastSignificantBits);

    }

    public void writeIPV4Address(Inet4Address value) throws IOException {
        BinaryStreamUtils.writeInet4Address(out, value);
    }

    public void writeIPV6Address(Inet6Address value) throws IOException {
        BinaryStreamUtils.writeInet6Address(out, value);
    }

    public static boolean writeValuePreamble(OutputStream out, boolean defaultsSupport, ClickHouseColumn column, Object value) throws IOException {
        return writeValuePreamble(out, defaultsSupport, value, column.isNullable(), column.getDataType(), column.hasDefault(), column.getColumnName());
    }

    public static boolean writeValuePreamble(OutputStream out, boolean defaultsSupport, Object value, boolean isNullable, ClickHouseDataType dataType, boolean hasDefault, String column) throws IOException {
        if (defaultsSupport) {
            if (value != null) {//Because we now support defaults, we have to send nonNull
                SerializerUtils.writeNonNull(out);//Write 0 for no default

                if (isNullable) {//If the column is nullable
                    SerializerUtils.writeNonNull(out);//Write 0 for not null
                }
            } else {//So if the object is null
                if (hasDefault) {
                    SerializerUtils.writeNull(out);//Send 1 for default
                    return false;
                } else if (isNullable) {//And the column is nullable
                    SerializerUtils.writeNonNull(out);
                    SerializerUtils.writeNull(out);//Then we send null, write 1
                    return false;//And we're done
                } else if (dataType == ClickHouseDataType.Array) {//If the column is an array
                    SerializerUtils.writeNonNull(out);//Then we send nonNull
                } else if (dataType == ClickHouseDataType.Dynamic) {
                    // do nothing
                } else {
                    throw new IllegalArgumentException(String.format("An attempt to write null into not nullable column '%s'", column));
                }
            }
        } else {
            if (isNullable) {
                if (value == null) {
                    SerializerUtils.writeNull(out);
                    return false;
                }
                SerializerUtils.writeNonNull(out);
            } else if (value == null) {
                if (dataType == ClickHouseDataType.Array) {
                    SerializerUtils.writeNonNull(out);
                } else if (dataType == ClickHouseDataType.Dynamic) {
                    // do nothing
                } else {
                    throw new IllegalArgumentException(String.format("An attempt to write null into not nullable column '%s'", column));
                }
            }
        }

        return true;
    }

    public static void writeSize(OutputStream out, long size) throws IOException {
        SerializerUtils.writeVarInt(out, size);
    }
}

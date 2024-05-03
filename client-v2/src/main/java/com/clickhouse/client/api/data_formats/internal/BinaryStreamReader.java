package com.clickhouse.client.api.data_formats.internal;

import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.format.BinaryStreamUtils;
import org.slf4j.Logger;
import org.slf4j.helpers.NOPLogger;

import java.io.EOFException;
import java.io.IOException;
import java.math.BigInteger;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.TimeZone;

public class BinaryStreamReader {

    private final ClickHouseInputStream chInputStream;

    private final Logger log;

    BinaryStreamReader(ClickHouseInputStream chInputStream, Logger log) {
        this.chInputStream = chInputStream;
        this.log = log == null ? NOPLogger.NOP_LOGGER : log;
    }

    public <T> T readValue(ClickHouseDataType dataType, TimeZone timeZone) throws IOException {
        return readValueImpl(dataType, timeZone);
    }

    private <T> T readValueImpl(ClickHouseDataType dataType, TimeZone timeZone) throws IOException {
        try {
            switch (dataType) {
                // Primitives
                case FixedString:
                    return (T) BinaryStreamUtils.readFixedString(chInputStream, dataType.getByteLength());
                case String: {
                    // TODO: BinaryStreamUtils.readString() - requires reader that may be causing EOF exception
                    int len = chInputStream.readVarInt();
                    return (T) chInputStream.readUnicodeString(len);
                }
                case Int8:
                    return (T) Byte.valueOf(BinaryStreamUtils.readInt8(chInputStream));
                case UInt8:
                    return (T) Short.valueOf(BinaryStreamUtils.readUnsignedInt8(chInputStream));
                case Int16:
                    return (T) Short.valueOf(BinaryStreamUtils.readInt16(chInputStream));
                case UInt16:
                    return (T) Integer.valueOf(BinaryStreamUtils.readUnsignedInt16(chInputStream));
                case Int32:
                    return (T) Integer.valueOf(BinaryStreamUtils.readInt32(chInputStream));
                case UInt32:
                    return (T) Long.valueOf(BinaryStreamUtils.readUnsignedInt32(chInputStream));
                case Int64:
                    return (T) Long.valueOf(BinaryStreamUtils.readInt64(chInputStream));
                case UInt64:
                    return (T) BinaryStreamUtils.readUnsignedInt64(chInputStream);
                case Int128:
                    return (T) BinaryStreamUtils.readInt128(chInputStream);
                case UInt128:
                    return (T) BinaryStreamUtils.readUnsignedInt128(chInputStream);
                case Int256:
                    return (T) BinaryStreamUtils.readInt256(chInputStream);
                case UInt256:
                    return (T) BinaryStreamUtils.readUnsignedInt256(chInputStream);
                case Decimal:
                    return (T) BinaryStreamUtils.readDecimal(chInputStream, dataType.getMaxPrecision(), dataType.getMaxScale());
                case Decimal32:
                    return (T) BinaryStreamUtils.readDecimal32(chInputStream, dataType.getMaxScale());
                case Decimal64:
                    return (T) BinaryStreamUtils.readDecimal64(chInputStream, dataType.getMaxScale());
                case Decimal128:
                    return (T) BinaryStreamUtils.readDecimal128(chInputStream, dataType.getMaxScale());
                case Decimal256:
                    return (T) BinaryStreamUtils.readDecimal256(chInputStream, dataType.getMaxScale());
                case Float32:
                    return (T) Float.valueOf(BinaryStreamUtils.readFloat32(chInputStream));
                case Float64:
                    return (T) Double.valueOf(BinaryStreamUtils.readFloat64(chInputStream));

                case Bool:
                    return (T) Boolean.valueOf(BinaryStreamUtils.readBoolean(chInputStream));
                case Enum8:
                    return (T) Byte.valueOf(BinaryStreamUtils.readEnum8(chInputStream));
                case Enum16:
                    return (T) Short.valueOf(BinaryStreamUtils.readEnum16(chInputStream));

                case Date:
                    return (T) BinaryStreamUtils.readDate(chInputStream, timeZone);
                case Date32:
                    return (T) BinaryStreamUtils.readDate32(chInputStream, timeZone);
                case DateTime:
                    return (T) BinaryStreamUtils.readDateTime(chInputStream, timeZone);
                case DateTime32:
                    return (T) BinaryStreamUtils.readDateTime32(chInputStream, timeZone);
                case DateTime64:
                    return (T) BinaryStreamUtils.readDateTime64(chInputStream, timeZone);

                case IntervalYear:
                case IntervalQuarter:
                case IntervalMonth:
                case IntervalWeek:
                case IntervalDay:
                case IntervalHour:
                case IntervalMinute:
                case IntervalSecond:
                case IntervalMicrosecond:
                case IntervalMillisecond:
                case IntervalNanosecond:
                    return (T) BinaryStreamUtils.readUnsignedInt64(chInputStream);

                case IPv4:
                    return (T) BinaryStreamUtils.readInet4Address(chInputStream);
                case IPv6:
                    return (T) BinaryStreamUtils.readInet6Address(chInputStream);
                case UUID:
                    return (T) BinaryStreamUtils.readUuid(chInputStream);
                case Point:
                    return (T) BinaryStreamUtils.readGeoPoint(chInputStream);
                case Polygon:
                    return (T) BinaryStreamUtils.readGeoPolygon(chInputStream);
                case MultiPolygon:
                    return (T) BinaryStreamUtils.readGeoMultiPolygon(chInputStream);
                case Ring:
                    return (T) BinaryStreamUtils.readGeoRing(chInputStream);

//                case JSON: // obsolete https://clickhouse.com/docs/en/sql-reference/data-types/json#displaying-json-column
//                case Object:
                case Array:
                    int len = chInputStream.readVarInt();
                    Object[] array = new Object[len];
                    return (T) array;
//                case Map:
//                case Nested:
//                case Tuple:

//                case Nothing:
//                    return null;
//                case SimpleAggregateFunction:
//                case AggregateFunction:
                default:
                    throw new IllegalArgumentException("Unsupported data type: " + dataType);
            }
        } catch (EOFException e) {
            throw e;
        } catch (IOException e) {
            // TODO: handle parse exception when stream is readable but data is not valid for the type
            log.error("Failed to read value of type: {}", dataType, e);
            throw new RuntimeException(e);
        }
    }
}

package com.clickhouse.client.api.data_formats.internal;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseValues;
import org.slf4j.Logger;
import org.slf4j.helpers.NOPLogger;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

public class BinaryStreamReader {

    private final DataInputStream input;

    private final Logger log;

    BinaryStreamReader(InputStream input, Logger log) {
        this.log = log == null ? NOPLogger.NOP_LOGGER : log;
        this.input = new DataInputStream(input); // wrap input stream
    }

    public <T> T readValue(ClickHouseColumn column) throws IOException {
        return readValueImpl(column);
    }

    private <T> T readValueImpl(ClickHouseColumn column) throws IOException {
        if (column.isNullable()) {

            if (input.readBoolean()) { // is Null?
                return (T) null;
            }
        }

        try {
            switch (column.getDataType()) {
                // Primitives
                case FixedString: {
                    return (T) new String(readNBytes(input, column.getEstimatedLength()), StandardCharsets.UTF_8);
                }
                case String: {
                    // TODO: BinaryStreamUtils.readString() - requires reader that may be causing EOF exception
                    int len =  readVarInt(input);
                    if ( len == 0 ) {
                        return (T) "";
                    }
                    return (T) new String(readNBytes(input, len), StandardCharsets.UTF_8);
                }
                case Int8:
                    return (T) Byte.valueOf(input.readByte());
                case UInt8:
                    return (T) Short.valueOf((short) input.readUnsignedByte());
                case Int16:
                    return (T) Short.valueOf(input.readShort());
                case UInt16:
                    return (T) Integer.valueOf(input.readUnsignedShort());
                case Int32:
                    return (T) Integer.valueOf(input.readInt());
                case UInt32:
                    return (T) Long.valueOf(input.readInt() & 0xFFFFFFFFL);
                case Int64:
                    return (T) Long.valueOf(input.readLong());
                case UInt64:
                    return (T) new BigInteger(readNBytes(input, 8));
                case Int128:
                    return (T) new BigInteger(readNBytes(input, 16));
                case UInt128:
                    return (T) new BigInteger(readNBytes(input, 16));
                case Int256:
                    return (T) new BigInteger(readNBytes(input, 32));
                case UInt256:
                    return (T) new BigInteger(readNBytes(input, 64));
                case Decimal:
                    return (T) readDecimal(input, column.getPrecision(), column.getScale());
                case Decimal32:
                    return (T) readDecimal(input, column.getPrecision(), column.getScale());
                case Decimal64:
                    return (T) readDecimal(input, column.getPrecision(), column.getScale());
                case Decimal128:
                    return (T) readDecimal(input, column.getPrecision(), column.getScale());
                case Decimal256:
                    return (T) readDecimal(input, column.getPrecision(), column.getScale());
                case Float32:
                    return (T) Float.valueOf(input.readFloat());
                case Float64:
                    return (T) Double.valueOf(input.readDouble());
                case Bool:
                    return (T) Boolean.valueOf(input.readBoolean());
                case Enum8:
                    return (T) Byte.valueOf(input.readByte());
                case Enum16:
                    return (T) Short.valueOf(input.readShort());

                case Date:
                    return (T) readDate(input, column.getTimeZone());
                case Date32:
                    return (T) readDate32(input, column.getTimeZone());
                case DateTime:
                    return (T) readDateTime32(input, column.getTimeZone());
                case DateTime32:
                    return (T) readDateTime32(input, column.getTimeZone());
                case DateTime64:
                    return (T) readDateTime64(input, 3, column.getTimeZone());

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
                    return (T) new BigInteger(readNBytes(input, 8));

                case IPv4:
                    return (T) Inet4Address.getByAddress(readNBytes(input, 4));
                case IPv6:
                    return (T) Inet6Address.getByAddress(readNBytes(input, 16));
                case UUID:
                    return (T) new UUID(input.readLong(), input.readLong());
                case Point:
                    return (T) readGeoPoint(input);
                case Polygon:
                    return (T) readGeoPolygon(input);
                case MultiPolygon:
                    return (T) readGeoMultiPolygon(input);
                case Ring:
                    return (T) readGeoRing(input);

//                case JSON: // obsolete https://clickhouse.com/docs/en/sql-reference/data-types/json#displaying-json-column
//                case Object:
                case Array:
                    return (T) readArray(column);
                case Map:
                    return (T) readMap(column);
//                case Nested:
                case Tuple:
                    return (T) readTuple(column);
                case Nothing:
                    return null;
//                case SimpleAggregateFunction:
//                case AggregateFunction:
                default:
                    throw new IllegalArgumentException("Unsupported data type: " + column.getDataType());
            }
        } catch (EOFException e) {
            throw e;
        } catch (IOException e) {
            // TODO: handle parse exception when stream is readable but data is not valid for the type
            log.error("Failed to read value of type: {}", column.getDataType(), e);
            throw new RuntimeException(e);
        }
    }

    public BigDecimal readDecimal(DataInputStream input, int precision, int scale) throws IOException {
        BigDecimal v;

        if (precision <= ClickHouseDataType.Decimal32.getMaxScale()) {
            return BigDecimal.valueOf(input.readInt(), scale);
        } else if (precision <= ClickHouseDataType.Decimal64.getMaxScale()) {
            v =  BigDecimal.valueOf(input.readLong(), scale);
        } else if (precision <= ClickHouseDataType.Decimal128.getMaxScale()) {
            v = new BigDecimal(new BigInteger(readNBytes(input, 16)), scale);
        } else {
            v = new BigDecimal(new BigInteger(readNBytes(input, 32)), scale);
        }

        return v;
    }

    private byte[] readNBytes(DataInputStream inputStream, int len) throws IOException {
        byte[] bytes = new byte[len];
        inputStream.readFully(bytes);
        return bytes;
    }

    private ArrayValue readArray(ClickHouseColumn column) throws IOException {
        Class<?> itemType = column.getArrayBaseColumn().getDataType().getWiderPrimitiveClass();
        int len = readVarInt(input);
        ArrayValue array = new ArrayValue(column.getArrayNestedLevel() > 1 ? ArrayValue.class : itemType, len);

        if (len == 0) {
            return array;
        }

        for (int i = 0; i < len; i++) {
            array.set(i, readValueImpl(column.getNestedColumns().get(0)));
        }

        return array;
    }

    public static class ArrayValue {

        final int length;

        final Class<?> itemType;

        final Object array;

        ArrayValue(Class<?> itemType, int length) {
            this.itemType = itemType;
            this.length = length;

            try {
                if (itemType.isArray()) {
                    array = Array.newInstance(ArrayValue.class, length);
                } else {
                    array = Array.newInstance(itemType, length);
                }
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to create array of type: " + itemType, e);
            }
        }

        public int length() {
            return length;
        }

        public Object get(int index) {
            return Array.get(array, index);
        }

        public void set(int index, Object value) {
            try {
                Array.set(array, index, value);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Failed to set value at index: " + index +
                        " value " + value + " of class " + value.getClass().getName(), e);
            }
        }
    }

    private Map<?,?> readMap(ClickHouseColumn column) throws IOException {
        int len = readVarInt(input);

        if (len == 0) {
            return Collections.emptyMap();
        }

        ClickHouseColumn keyType = column.getKeyInfo();
        ClickHouseColumn valueType = column.getValueInfo();
        LinkedHashMap<Object, Object> map = new LinkedHashMap<>(len);
        for (int i = 0; i < len; i++) {
            Object key = readValueImpl(keyType);
            Object value = readValueImpl(valueType);
            map.put(key, value);
        }

        return map;
    }

    private Object[] readTuple(ClickHouseColumn column) throws IOException {
        int len = column.getNestedColumns().size();
        Object[] tuple = new Object[len];
        for (int i = 0; i < len; i++) {
            tuple[i] = readValueImpl(column.getNestedColumns().get(i));
        }

        return tuple;
    }

    public double[] readGeoPoint(DataInputStream input) throws IOException {
        return new double[] { input.readDouble(), input.readDouble() };
    }

    public double[][] readGeoRing(DataInputStream input) throws IOException {
        int count = readVarInt(input);
        double[][] value = new double[count][2];
        for (int i = 0; i < count; i++) {
            value[i] = readGeoPoint(input);
        }
        return value;
    }


    public double[][][] readGeoPolygon(DataInputStream input) throws IOException {
        int count = readVarInt(input);
        double[][][] value = new double[count][][];
        for (int i = 0; i < count; i++) {
            value[i] = readGeoRing(input);
        }
        return value;
    }

    private double[][][][] readGeoMultiPolygon(DataInputStream input) throws IOException {
        int count = readVarInt(input);
        double[][][][] value = new double[count][][][];
        for (int i = 0; i < count; i++) {
            value[i] = readGeoPolygon(input);
        }
        return value;
    }

    /**
     * Reads a varint from input stream.
     *
     * @return varint
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    private int readVarInt(DataInputStream input) throws IOException {
        // https://github.com/ClickHouse/ClickHouse/blob/abe314feecd1647d7c2b952a25da7abf5c19f352/src/IO/VarInt.h#L126
        int b = input.readByte();
        if (b >= 0) {
            return b;
        }

        int result = b & 0x7F;
        for (int shift = 7; shift <= 28; shift += 7) {
            if ((b = input.readByte()) >= 0) {
                result |= b << shift;
                break;
            } else {
                result |= (b & 0x7F) << shift;
            }
        }
        // consume a few more bytes - readVarLong() should be called instead
//        if (b < 0) {
//            for (int shift = 35; shift <= 63; shift += 7) {
//                if (peek() < 0 || readByte() >= 0) {
//                    break;
//                }
//            }
//        }
        return result;
    }

    /**
     * Reads 64-bit varint as long from input stream.
     *
     * @return 64-bit varint
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public long readVarLong(DataInputStream input) throws IOException {
        long b = input.readByte();
        if (b >= 0L) {
            return b;
        }

        long result = b & 0x7F;
        for (int shift = 7; shift <= 63; shift += 7) {
            if ((b = input.readByte()) >= 0) {
                result |= b << shift;
                break;
            } else {
                result |= (b & 0x7F) << shift;
            }
        }
        return result;
    }

    public static LocalDate readDate(DataInputStream input, TimeZone tz)
            throws IOException {
        LocalDate d = LocalDate.ofEpochDay(input.readUnsignedShort());
        if (tz != null && !tz.toZoneId().equals(ClickHouseValues.SYS_ZONE)) {
            d = d.atStartOfDay(ClickHouseValues.SYS_ZONE).withZoneSameInstant(tz.toZoneId()).toLocalDate();
        }
        return d;
    }

    public static LocalDate readDate32(DataInputStream input, TimeZone tz)
            throws IOException {
        LocalDate d = LocalDate.ofEpochDay(input.readInt());
        if (tz != null && !tz.toZoneId().equals(ClickHouseValues.SYS_ZONE)) {
            d = d.atStartOfDay(ClickHouseValues.SYS_ZONE).withZoneSameInstant(tz.toZoneId()).toLocalDate();
        }
        return d;
    }

    public static LocalDateTime readDateTime32(DataInputStream input, TimeZone tz) throws IOException {
        long time = input.readLong();

        return LocalDateTime.ofInstant(Instant.ofEpochSecond(time < 0L ? 0L : time),
                tz != null ? tz.toZoneId() : ClickHouseValues.UTC_ZONE);
    }
    private static final int[] BASES = new int[] { 1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000,
            1000000000 };

    public static LocalDateTime readDateTime64(DataInputStream input, int scale, TimeZone tz) throws IOException {
        long value = input.readLong();
        int nanoSeconds = 0;
        if (scale > 0) {
            int factor = BASES[scale];
            nanoSeconds = (int) (value % factor);
            value /= factor;
            if (nanoSeconds < 0) {
                nanoSeconds += factor;
                value--;
            }
            if (nanoSeconds > 0L) {
                nanoSeconds *= BASES[9 - scale];
            }
        }

        return LocalDateTime.ofInstant(Instant.ofEpochSecond(value, nanoSeconds),
                tz != null ? tz.toZoneId() : TimeZone.getTimeZone("UTC").toZoneId());
    }
}

package com.clickhouse.client.api.data_formats.internal;

import com.clickhouse.client.api.ClientException;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseValues;
import org.slf4j.Logger;
import org.slf4j.helpers.NOPLogger;

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
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

public class BinaryStreamReader {

    private final InputStream input;

    private final Logger log;

    private final TimeZone timeZone;

    BinaryStreamReader(InputStream input, TimeZone timeZone, Logger log) {
        this.log = log == null ? NOPLogger.NOP_LOGGER : log;
        this.timeZone = timeZone;
        this.input = input;
    }

    public <T> T readValue(ClickHouseColumn column) throws IOException {
        return readValueImpl(column);
    }

    private <T> T readValueImpl(ClickHouseColumn column) throws IOException {
        if (column.isNullable()) {
            int isNull = readByteOrEOF(input);
            if (isNull == 1) { // is Null?
                return (T) null;
            }
        }

        try {
            switch (column.getDataType()) {
                // Primitives
                case FixedString: {
                    byte[] bytes = readNBytes(input, column.getEstimatedLength());
                    int end = 0;
                    for (int i = 0; i < bytes.length; i++) {
                        if (bytes[i] == 0) {
                            end = i;
                            break;
                        }
                    }
                    return (T) new String(bytes, 0, end, StandardCharsets.UTF_8);
                }
                case String: {
                    int len = readVarInt(input);
                    if (len == 0) {
                        return (T) "";
                    }
                    return (T) new String(readNBytes(input, len), StandardCharsets.UTF_8);
                }
                case Int8:
                    return (T) Byte.valueOf((byte) readByteOrEOF(input));
                case UInt8:
                    return (T) Short.valueOf(readUnsignedByte(input));
                case Int16:
                    return (T) Short.valueOf(readShortLE(input));
                case UInt16:
                    return (T) Integer.valueOf(readUnsignedShortLE(input));
                case Int32:
                    return (T) Integer.valueOf(readIntLE(input));
                case UInt32:
                    return (T) Long.valueOf(readUnsignedIntLE(input));
                case Int64:
                    return (T) Long.valueOf(readLongLE(input));
                case UInt64:
                    return (T) readUnsignedInt64LE(input);
                case Int128:
                    return (T) readInt128LE(input);
                case UInt128:
                    return (T) readUnsignedInt128LE(input);
                case Int256:
                    return (T) readInt256LE(input);
                case UInt256:
                    return (T) readUnsignedInt256LE(input);
                case Decimal:
                    return (T) readDecimal(input, column.getPrecision(), column.getScale());
                case Decimal32:
                    return (T) readDecimal32(input, column.getScale());
                case Decimal64:
                    return (T) readDecimal64(input, column.getScale());
                case Decimal128:
                    return (T) readDecimal128(input, column.getScale());
                case Decimal256:
                    return (T) readDecimal256(input, column.getScale());
                case Float32:
                    return (T) Float.valueOf(readFloatLE(input));
                case Float64:
                    return (T) Double.valueOf(readDoubleLE(input));
                case Bool:
                    return (T) Boolean.valueOf(readByteOrEOF(input) == 1);
                case Enum8:
                    return (T) Byte.valueOf((byte) readUnsignedByte(input));
                case Enum16:
                    return (T) Short.valueOf((short) readUnsignedShortLE(input));
                case Date:
                    return (T) readDate(input, column.getTimeZone() == null ? timeZone:
                            column.getTimeZone());
                case Date32:
                    return (T) readDate32(input, column.getTimeZone() == null ? timeZone:
                            column.getTimeZone());
                case DateTime:
                    return (T) readDateTime32(input, column.getTimeZone() == null ? timeZone:
                            column.getTimeZone());
                case DateTime32:
                    return (T) readDateTime32(input, column.getTimeZone() == null ? timeZone:
                            column.getTimeZone());
                case DateTime64:
                    return (T) readDateTime64(input, 3, column.getTimeZone() == null ? timeZone:
                            column.getTimeZone());

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
                    return (T) readBigIntegerLE(input, 8, true);
                case IPv4:
                    https://clickhouse.com/docs/en/sql-reference/data-types/ipv4
                    return (T) Inet4Address.getByAddress(readNBytesLE(input, 4));
                case IPv6:
                    // https://clickhouse.com/docs/en/sql-reference/data-types/ipv6
                    return (T) Inet6Address.getByAddress(readNBytes(input, 16));
                case UUID:
                    return (T) new UUID(readLongLE(input), readLongLE(input));
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
        } catch (Exception e) {
            throw new ClientException("Failed to read value for column " + column.getColumnName(), e);
        }
    }

    public static short readShortLE(InputStream input) throws IOException {
        short v = 0;
        v |= (short) readByteOrEOF(input);
        v |= (short) (readByteOrEOF(input) << 8);
        return v;
    }

    public static int readIntLE(InputStream input) throws IOException {
        int v = 0;
        v |= readByteOrEOF(input);
        v |= readByteOrEOF(input) << 8;
        v |= readByteOrEOF(input) << 16;
        v |= readByteOrEOF(input) << 24;
        return v;
    }

    public static long readLongLE(InputStream input) throws IOException {
        long v = 0;
        v |= readByteOrEOF(input);
        v |= (0xFFL & readByteOrEOF(input)) << 8;
        v |= (0xFFL & readByteOrEOF(input)) << 16;
        v |= (0xFFL & readByteOrEOF(input)) << 24;
        v |= (0xFFL & readByteOrEOF(input)) << 32;
        v |= (0xFFL & readByteOrEOF(input)) << 40;
        v |= (0xFFL & readByteOrEOF(input)) << 48;
        v |= (0xFFL & readByteOrEOF(input)) << 56;

        return v;
    }


    public static BigInteger readBigIntegerLE(InputStream input, int len, boolean unsigned) throws IOException {
        byte[] bytes = readNBytes(input, len);
        int s = 0;
        int i = len - 1;
        while (s < i) {
            byte b = bytes[s];
            bytes[s] = bytes[i];
            bytes[i] = b;
            s++;
            i--;
        }

        return unsigned ? new BigInteger(1, bytes) : new BigInteger(bytes);
    }

    public static BigInteger readInt128LE(InputStream input) throws IOException {
        return readBigIntegerLE(input, 16, false);
    }

    public static BigInteger readInt256LE(InputStream input) throws IOException {
        return readBigIntegerLE(input, 32, false);
    }

    public static float readFloatLE(InputStream input) throws IOException {
        return Float.intBitsToFloat(readIntLE(input));
    }

    public static double readDoubleLE(InputStream input) throws IOException {
        return Double.longBitsToDouble(readLongLE(input));
    }

    public static BigDecimal readDecimal(InputStream input, int precision, int scale) throws IOException {
        BigDecimal v;

        if (precision <= ClickHouseDataType.Decimal32.getMaxScale()) {
            return BigDecimal.valueOf(readIntLE(input), scale);
        } else if (precision <= ClickHouseDataType.Decimal64.getMaxScale()) {
            v =  BigDecimal.valueOf(readLongLE(input), scale);
        } else if (precision <= ClickHouseDataType.Decimal128.getMaxScale()) {
            v = new BigDecimal(readBigIntegerLE(input, 16, false), scale);
        } else {
            v = new BigDecimal(readBigIntegerLE(input, 32, false), scale);
        }

        return v;
    }

    public static BigDecimal readDecimal32(InputStream input, int scale) throws IOException {
        return BigDecimal.valueOf(readIntLE(input), scale);
    }

    public static BigDecimal readDecimal64(InputStream input, int scale) throws IOException {
        return BigDecimal.valueOf(readLongLE(input), scale);
    }

    public static BigDecimal readDecimal128(InputStream input, int scale) throws IOException {
        return new BigDecimal(readInt128LE(input), scale);
    }

    public static BigDecimal readDecimal256(InputStream input, int scale) throws IOException {
        return new BigDecimal(readInt256LE(input), scale);
    }

    public static byte[] readNBytes(InputStream inputStream, int len) throws IOException {
        byte[] bytes = new byte[len];
        int total = 0;
        while (total < len) {
            int r = inputStream.read(bytes, total, len - total);
            if (r == -1) {
                throw new EOFException("End of stream reached before reading all data");
            }
            total += r;
        }
        return bytes;
    }

    public static byte[] readNBytesLE(InputStream input, int len) throws IOException {
        byte[] bytes = readNBytes(input, len);

        int s = 0;
        int i = len - 1;
        while (s < i) {
            byte b = bytes[s];
            bytes[s] = bytes[i];
            bytes[i] = b;
            s++;
            i--;
        }

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

        public <T> List<T> asList() {
            ArrayList<T> list = new ArrayList<>(length);
            for (int i = 0; i < length; i++) {
                list.add((T) get(i));
            }
            return list;
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

    public static double[] readGeoPoint(InputStream input) throws IOException {
        return new double[] { readDoubleLE(input), readDoubleLE(input) };
    }

    public static double[][] readGeoRing(InputStream input) throws IOException {
        int count = readVarInt(input);
        double[][] value = new double[count][2];
        for (int i = 0; i < count; i++) {
            value[i] = readGeoPoint(input);
        }
        return value;
    }


    public double[][][] readGeoPolygon(InputStream input) throws IOException {
        int count = readVarInt(input);
        double[][][] value = new double[count][][];
        for (int i = 0; i < count; i++) {
            value[i] = readGeoRing(input);
        }
        return value;
    }

    private double[][][][] readGeoMultiPolygon(InputStream input) throws IOException {
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
    public static int readVarInt(InputStream input) throws IOException {
        int value = 0;

        for (int i = 0 ; i < 10 ; i++) {
            byte b = (byte) readByteOrEOF(input);
            value |= (b & 0x7F) << (7 * i);

            if ((b & 0x80) == 0) {
                break;
            }
        }

        return value;
    }

    public static short readUnsignedByte(InputStream input) throws IOException {
        return (short) (readByteOrEOF(input) & 0xFF);
    }

    public static int readUnsignedShortLE(InputStream input) throws IOException {
        return readShortLE(input) & 0xFFFF;
    }

    public static long readUnsignedIntLE(InputStream input) throws IOException {
        return readIntLE(input) & 0xFFFFFFFFL;
    }

    public static BigInteger readUnsignedInt64LE(InputStream input) throws IOException {
        return readBigIntegerLE(input, 8, true);
    }

    public static BigInteger readUnsignedInt128LE(InputStream input) throws IOException {
        return readBigIntegerLE(input, 16, true);
    }

    public static BigInteger readUnsignedInt256LE(InputStream input) throws IOException {
        return readBigIntegerLE(input, 32, true);
    }

    public static ZonedDateTime readDate(InputStream input, TimeZone tz) throws IOException {
        LocalDate d = LocalDate.ofEpochDay(readUnsignedShortLE(input));
        return d.atStartOfDay(tz.toZoneId()).withZoneSameInstant(tz.toZoneId());
    }

    public static ZonedDateTime readDate32(InputStream input, TimeZone tz)
            throws IOException {
        LocalDate d = LocalDate.ofEpochDay(readIntLE(input));
        return d.atStartOfDay(tz.toZoneId()).withZoneSameInstant(tz.toZoneId());
    }

    public static ZonedDateTime readDateTime32(InputStream input, TimeZone tz) throws IOException {
        long time = readUnsignedIntLE(input);
        return LocalDateTime.ofInstant(Instant.ofEpochSecond(Math.max(time, 0L)), tz.toZoneId()).atZone(tz.toZoneId());
    }
    private static final int[] BASES = new int[] { 1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000,
            1000000000 };

    public static ZonedDateTime readDateTime64(InputStream input, int scale, TimeZone tz) throws IOException {
        long value = readLongLE(input);
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

        return LocalDateTime.ofInstant(Instant.ofEpochSecond(value, nanoSeconds), tz.toZoneId())
                .atZone(tz.toZoneId());
    }

    public static String readString(InputStream input) throws IOException {
        int len =  readVarInt(input);
        if ( len == 0 ) {
            return "";
        }
        return new String(readNBytes(input, len), StandardCharsets.UTF_8);
    }

    private static int readByteOrEOF(InputStream input) throws IOException {
        int b = input.read();
        if (b < 0) {
            throw new EOFException("End of stream reached before reading all data");
        }
        return b;
    }
}

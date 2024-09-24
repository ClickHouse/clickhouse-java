package com.clickhouse.client.api.data_formats.internal;

import com.clickhouse.client.api.ClientException;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.format.BinaryStreamUtils;
import com.clickhouse.data.value.ClickHouseBitmap;
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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

/**
 * This class is not thread safe and should not be shared between multiple threads.
 * Internally it may use a shared buffer to read data from the input stream.
 * It is done mainly to reduce extra memory allocations for reading numbers.
 */
public class BinaryStreamReader {

    private final InputStream input;

    private final Logger log;

    private final TimeZone timeZone;

    private final ByteBufferAllocator bufferAllocator;

    /**
     * Creates a BinaryStreamReader instance that will use {@link DefaultByteBufferAllocator} to allocate buffers.
     *
     * @param input - source of raw data in a suitable format
     * @param timeZone - timezone to use for date and datetime values
     * @param log - logger
     */
    BinaryStreamReader(InputStream input, TimeZone timeZone, Logger log) {
        this(input, timeZone, log, new DefaultByteBufferAllocator());
    }

    /**
     * Createa a BinaryStreamReader instance that will use the provided buffer allocator.
     *
     * @param input - source of raw data in a suitable format
     * @param timeZone - timezone to use for date and datetime values
     * @param log - logger
     * @param bufferAllocator - byte buffer allocator
     */
    BinaryStreamReader(InputStream input, TimeZone timeZone, Logger log, ByteBufferAllocator bufferAllocator) {
        this.log = log == null ? NOPLogger.NOP_LOGGER : log;
        this.timeZone = timeZone;
        this.input = input;
        this.bufferAllocator = bufferAllocator;
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
                    return (T) new String(bytes, 0, column.getEstimatedLength(), StandardCharsets.UTF_8);
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
                    return (T) readBigIntegerLE(input, INT64_SIZE, true);
                case Int128:
                    return (T) readBigIntegerLE(input, INT128_SIZE, false);
                case UInt128:
                    return (T) readBigIntegerLE(input, INT128_SIZE, true);
                case Int256:
                    return (T) readBigIntegerLE(input, INT256_SIZE, false);
                case UInt256:
                    return (T) readBigIntegerLE(input, INT256_SIZE, true);
                case Decimal:
                    return (T) readDecimal(input, column.getPrecision(), column.getScale());
                case Decimal32:
                    return (T) readDecimal(input, ClickHouseDataType.Decimal32.getMaxPrecision(), column.getScale());
                case Decimal64:
                    return (T) readDecimal(input, ClickHouseDataType.Decimal64.getMaxPrecision(), column.getScale());
                case Decimal128:
                    return (T) readDecimal(input, ClickHouseDataType.Decimal128.getMaxPrecision(), column.getScale());
                case Decimal256:
                    return (T) readDecimal(input, ClickHouseDataType.Decimal256.getMaxPrecision(), column.getScale());
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
                    return (T) readDate(input, column.getTimeZone() == null ? timeZone :
                            column.getTimeZone());
                case Date32:
                    return (T) readDate32(input, column.getTimeZone() == null ? timeZone :
                            column.getTimeZone());
                case DateTime:
                    return (T) readDateTime32(input, column.getTimeZone() == null ? timeZone :
                            column.getTimeZone());
                case DateTime32:
                    return (T) readDateTime32(input, column.getTimeZone() == null ? timeZone :
                            column.getTimeZone());
                case DateTime64:
                    return (T) readDateTime64(input, 3, column.getTimeZone() == null ? timeZone :
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
                    // https://clickhouse.com/docs/en/sql-reference/data-types/ipv4
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
                case AggregateFunction:
                    return (T) ClickHouseBitmap.deserialize(input, column.getNestedColumns().get(0).getDataType());
                default:
                    throw new IllegalArgumentException("Unsupported data type: " + column.getDataType());
            }
        } catch (EOFException e) {
            log.info("End of stream reached before reading all data for column " + column.getColumnName());
            throw e;
        } catch (Exception e) {
            throw new ClientException("Failed to read value for column " + column.getColumnName(), e);
        }
    }

    private short readShortLE(InputStream input) throws IOException {
        return readShortLE(input, bufferAllocator.allocate(INT16_SIZE));
    }

    /**
     * Reads a little-endian short from input stream. Uses buff to receive data from the input stream.
     *
     * @param input - source of bytes
     * @param buff  - buffer to store data
     * @return short value
     * @throws IOException
     */
    public static short readShortLE(InputStream input, byte[] buff) throws IOException {
        readNBytes(input, buff, 0, 2);
        return (short) (buff[0] & 0xFF | (buff[1] & 0xFF) << 8);
    }

    private int readIntLE(InputStream input) throws IOException {
        return readIntLE(input, bufferAllocator.allocate(INT32_SIZE));
    }

    /**
     * Reads a little-endian int from input stream. Uses buff to receive data from the input stream.
     *
     * @param input - source of bytes
     * @param buff  - buffer to store data
     * @return - int value
     * @throws IOException
     */
    public static int readIntLE(InputStream input, byte[] buff) throws IOException {
        readNBytes(input, buff, 0, 4);
        return (buff[0] & 0xFF) | (buff[1] & 0xFF) << 8 | (buff[2] & 0xFF) << 16 | (buff[3] & 0xFF) << 24;
    }

    private long readLongLE(InputStream input) throws IOException {
        return readLongLE(input, bufferAllocator.allocate(INT64_SIZE));
    }

    /**
     * Reads a little-endian long from input stream. Uses buff to receive data from the input stream.
     *
     * @param input - source of bytes
     * @param buff  - buffer to store data
     * @return - long value
     * @throws IOException
     */
    public static long readLongLE(InputStream input, byte[] buff) throws IOException {
        readNBytes(input, buff, 0, 8);
        return (long) (buff[0] & 0xFF) | (long) (buff[1] & 0xFF) << 8 | (long) (buff[2] & 0xFF) << 16
                | (long) (buff[3] & 0xFF) << 24 | (long) (buff[4] & 0xFF) << 32 | (long) (buff[5] & 0xFF) << 40
                | (long) (buff[6] & 0xFF) << 48 | (long) (buff[7] & 0xFF) << 56;
    }

    public short readUnsignedByte(InputStream input) throws IOException {
        return (short) (readByteOrEOF(input) & 0xFF);
    }

    private int readUnsignedShortLE(InputStream input) throws IOException {
        return readUnsignedShortLE(input, bufferAllocator.allocate(INT16_SIZE));
    }

    /**
     * Reads a little-endian unsigned short from input stream. Uses buff to receive data from the input stream.
     *
     * @param input - source of bytes
     * @param buff - buffer to store data
     * @return - unsigned short value
     * @throws IOException
     */
    public static int readUnsignedShortLE(InputStream input, byte[] buff) throws IOException {
        return readShortLE(input, buff) & 0xFFFF;
    }

    private long readUnsignedIntLE(InputStream input) throws IOException {
        return readIntLE(input) & 0xFFFFFFFFL;
    }

    /**
     * Reads a little-endian unsigned int from input stream. Uses buff to receive data from the input stream.
     *
     * @param input - source of bytes
     * @param buff - buffer to store data
     * @return
     * @throws IOException
     */
    public static long readUnsignedIntLE(InputStream input, byte[] buff) throws IOException {
        return readIntLE(input, buff) & 0xFFFFFFFFL;
    }

    private BigInteger readBigIntegerLE(InputStream input, int len, boolean unsigned) throws IOException {
        return readBigIntegerLE(input, bufferAllocator.allocate(len), len, unsigned);
    }

    public static final int INT16_SIZE = 2;
    public static final int INT32_SIZE = 4;

    public static final int INT64_SIZE = 8;

    public static final int INT128_SIZE = 16;

    public static final int INT256_SIZE = 32;

    /**
     * Reads a little-endian big integer from input stream. Uses buff to receive data from the input stream.
     *
     * @param input - source of bytes
     * @param buff - buffer to store data
     * @param len - number of bytes to read
     * @param unsigned - whether the value is unsigned
     * @return - big integer value
     * @throws IOException
     */
    public static BigInteger readBigIntegerLE(InputStream input, byte[] buff, int len, boolean unsigned) throws IOException {
        byte[] bytes = readNBytesLE(input, buff, 0, len);
        return unsigned ? new BigInteger(1, bytes) : new BigInteger(bytes);
    }

    public float readFloatLE(InputStream input) throws IOException {
        return Float.intBitsToFloat(readIntLE(input));
    }

    public double readDoubleLE(InputStream input) throws IOException {
        return Double.longBitsToDouble(readLongLE(input));
    }

    private BigDecimal readDecimal(InputStream input, int precision, int scale) throws IOException {
        BigDecimal v;

        if (precision <= ClickHouseDataType.Decimal32.getMaxScale()) {
            return BigDecimal.valueOf(readIntLE(input), scale);
        } else if (precision <= ClickHouseDataType.Decimal64.getMaxScale()) {
            v = BigDecimal.valueOf(readLongLE(input), scale);
        } else if (precision <= ClickHouseDataType.Decimal128.getMaxScale()) {
            v = new BigDecimal(readBigIntegerLE(input, INT128_SIZE, false), scale);
        } else {
            v = new BigDecimal(readBigIntegerLE(input, INT256_SIZE, false), scale);
        }

        return v;
    }

    public static byte[] readNBytes(InputStream inputStream, int len) throws IOException {
        byte[] bytes = new byte[len];
        return readNBytes(inputStream, bytes, 0, len);
    }

    /**
     * Reads {@code len} bytes from input stream to buffer.
     *
     * @param inputStream - source of bytes
     * @param buffer      - target buffer
     * @param offset      - target buffer offset
     * @param len         - number of bytes to read
     * @return target buffer
     * @throws IOException
     */
    public static byte[] readNBytes(InputStream inputStream, byte[] buffer, int offset, int len) throws IOException {
        int total = 0;
        while (total < len) {
            int r = inputStream.read(buffer, offset + total, len - total);
            if (r == -1) {
                throw new EOFException("End of stream reached before reading all data");
            }
            total += r;
        }
        return buffer;
    }

    private byte[] readNBytesLE(InputStream input, int len) throws IOException {
        return readNBytesLE(input, bufferAllocator.allocate(len), 0, len);
    }

    /**
     * Reads {@code len} bytes from input stream to buffer in little-endian order.
     *
     * @param input  - source of bytes
     * @param buffer - target buffer
     * @param offset - target buffer offset
     * @param len    - number of bytes to read
     * @return - target buffer
     * @throws IOException
     */
    public static byte[] readNBytesLE(InputStream input, byte[] buffer, int offset, int len) throws IOException {
        byte[] bytes = readNBytes(input, buffer, 0, len);
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

        private List<?> list = null;

        public synchronized <T> List<T> asList() {
            if (list == null) {
                ArrayList<T> list = new ArrayList<>(length);
                for (int i = 0; i < length; i++) {
                    Object item = get(i);
                    if (item instanceof ArrayValue) {
                        list.add((T) ((ArrayValue) item).asList());
                    } else {
                        list.add((T) item);
                    }
                }
                this.list = list;
            }
            return (List<T>) list;
        }
    }

    private Map<?, ?> readMap(ClickHouseColumn column) throws IOException {
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

    private double[] readGeoPoint(InputStream input) throws IOException {
        return new double[]{readDoubleLE(input), readDoubleLE(input)};
    }

    private double[][] readGeoRing(InputStream input) throws IOException {
        int count = readVarInt(input);
        double[][] value = new double[count][2];
        for (int i = 0; i < count; i++) {
            value[i] = readGeoPoint(input);
        }
        return value;
    }


    private double[][][] readGeoPolygon(InputStream input) throws IOException {
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

        for (int i = 0; i < 10; i++) {
            byte b = (byte) readByteOrEOF(input);
            value |= (b & 0x7F) << (7 * i);

            if ((b & 0x80) == 0) {
                break;
            }
        }

        return value;
    }

    private ZonedDateTime readDate(InputStream input, TimeZone tz) throws IOException {
        return readDate(input, bufferAllocator.allocate(INT16_SIZE), tz);
    }

    /**
     * Reads a date from input stream.
     * @param input - source of bytes
     * @param buff - for reading short value. Should be 2 bytes.
     * @param tz - timezone
     * @return
     * @throws IOException
     */
    public static ZonedDateTime readDate(InputStream input, byte[] buff, TimeZone tz) throws IOException {
        LocalDate d = LocalDate.ofEpochDay(readUnsignedShortLE(input, buff));
        return d.atStartOfDay(tz.toZoneId()).withZoneSameInstant(tz.toZoneId());
    }

    private ZonedDateTime readDate32(InputStream input, TimeZone tz)
            throws IOException {
        return readDate32(input, bufferAllocator.allocate(INT32_SIZE), tz);
    }

    /**
     * Reads a date32 from input stream.
     *
     * @param input - source of bytes
     * @param buff - for reading int value. Should be 4 bytes.
     * @param tz - timezone
     * @return
     * @throws IOException
     */
    public static ZonedDateTime readDate32(InputStream input, byte[] buff, TimeZone tz)
            throws IOException {
        LocalDate d = LocalDate.ofEpochDay(readIntLE(input, buff));
        return d.atStartOfDay(tz.toZoneId()).withZoneSameInstant(tz.toZoneId());
    }

    private ZonedDateTime readDateTime32(InputStream input, TimeZone tz) throws IOException {
        return readDateTime32(input, bufferAllocator.allocate(INT32_SIZE), tz);
    }

    /**
     * Reads a datetime32 from input stream.
     * @param input - source of bytes
     * @param buff - for reading int value. Should be 4 bytes.
     * @param tz - timezone
     * @return ZonedDateTime
     * @throws IOException
     */
    public static ZonedDateTime readDateTime32(InputStream input, byte[] buff, TimeZone tz) throws IOException {
        long time = readUnsignedIntLE(input, buff);
        return LocalDateTime.ofInstant(Instant.ofEpochSecond(Math.max(time, 0L)), tz.toZoneId()).atZone(tz.toZoneId());
    }

    private ZonedDateTime readDateTime64(InputStream input, int scale, TimeZone tz) throws IOException {
        return readDateTime64(input, bufferAllocator.allocate(INT64_SIZE), scale, tz);
    }

    public static final int[] BASES = new int[]{1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000,
            1000000000};

    /**
     * Reads a datetime64 from input stream.
     *
     * @param input - source of bytes
     * @param buff - for reading long value. Should be 8 bytes.
     * @param scale - scale of the datetime64
     * @param tz - timezone
     * @return
     * @throws IOException
     */
    public static ZonedDateTime readDateTime64(InputStream input, byte[] buff, int scale, TimeZone tz) throws IOException {
        long value = readLongLE(input, buff);
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
        int len = readVarInt(input);
        if (len == 0) {
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

    public interface ByteBufferAllocator {
        byte[] allocate(int size);
    }

    /**
     * Byte allocator that creates a new byte array for each request.
     */
    public static class DefaultByteBufferAllocator implements ByteBufferAllocator {
        @Override
        public byte[] allocate(int size) {
            return new byte[size];
        }
    }

    /**
     * Byte allocator that caches preallocated byte arrays for small sizes.
     */
    public static class CachingByteBufferAllocator implements ByteBufferAllocator {

        private static final int MAX_PREALLOCATED_SIZE = 32;
        private final byte[][] preallocated = new byte[MAX_PREALLOCATED_SIZE + 1][];

        public CachingByteBufferAllocator() {
           for (int i = 0; i < preallocated.length; i++) {
               preallocated[i] = new byte[i];
           }
        }

        @Override
        public byte[] allocate(int size) {
            if (size < preallocated.length) {
                return preallocated[size];
            }

            return new byte[size];
        }
    }
}

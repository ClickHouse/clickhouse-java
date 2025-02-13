package com.clickhouse.client.api.data_formats.internal;

import com.clickhouse.client.api.ClientException;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
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

    private final boolean jsonAsString;

    /**
     * Createa a BinaryStreamReader instance that will use the provided buffer allocator.
     *
     * @param input - source of raw data in a suitable format
     * @param timeZone - timezone to use for date and datetime values
     * @param log - logger
     * @param bufferAllocator - byte buffer allocator
     */
    BinaryStreamReader(InputStream input, TimeZone timeZone, Logger log, ByteBufferAllocator bufferAllocator, boolean jsonAsString) {
        this.log = log == null ? NOPLogger.NOP_LOGGER : log;
        this.timeZone = timeZone;
        this.input = input;
        this.bufferAllocator = bufferAllocator;
        this.jsonAsString = jsonAsString;
    }

    /**
     * Reads a value from the internal input stream.
     * @param column - column information
     * @return value
     * @param <T> - target type of the value
     * @throws IOException when IO error occurs
     */
    public <T> T readValue(ClickHouseColumn column) throws IOException {
        return readValue(column, null);
    }

    /**
     * Reads a value from the internal input stream. Method will use type hint to do smarter conversion if possible.
     * For example, all datetime values are of {@link ZonedDateTime}; if a type hint is {@link LocalDateTime} then
     * {@link ZonedDateTime#toLocalDateTime()} .
     * @param column - column information
     * @param typeHint - type hint
     * @return value
     * @param <T> - target type of the value
     * @throws IOException when IO error occurs
     */
    @SuppressWarnings("unchecked")
    public <T> T readValue(ClickHouseColumn column, Class<?> typeHint) throws IOException {
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
                    return (T) Byte.valueOf(readByte());
                case UInt8:
                    return (T) Short.valueOf(readUnsignedByte());
                case Int16:
                    return (T) Short.valueOf(readShortLE());
                case UInt16:
                    return (T) Integer.valueOf(readUnsignedShortLE());
                case Int32:
                    return (T) Integer.valueOf(readIntLE());
                case UInt32:
                    return (T) Long.valueOf(readUnsignedIntLE());
                case Int64:
                    return (T) Long.valueOf(readLongLE());
                case UInt64:
                    return (T) readBigIntegerLE(INT64_SIZE, true);
                case Int128:
                    return (T) readBigIntegerLE(INT128_SIZE, false);
                case UInt128:
                    return (T) readBigIntegerLE(INT128_SIZE, true);
                case Int256:
                    return (T) readBigIntegerLE(INT256_SIZE, false);
                case UInt256:
                    return (T) readBigIntegerLE(INT256_SIZE, true);
                case Decimal:
                    return (T) readDecimal(column.getPrecision(), column.getScale());
                case Decimal32:
                    return (T) readDecimal(ClickHouseDataType.Decimal32.getMaxPrecision(), column.getScale());
                case Decimal64:
                    return (T) readDecimal(ClickHouseDataType.Decimal64.getMaxPrecision(), column.getScale());
                case Decimal128:
                    return (T) readDecimal(ClickHouseDataType.Decimal128.getMaxPrecision(), column.getScale());
                case Decimal256:
                    return (T) readDecimal(ClickHouseDataType.Decimal256.getMaxPrecision(), column.getScale());
                case Float32:
                    return (T) Float.valueOf(readFloatLE());
                case Float64:
                    return (T) Double.valueOf(readDoubleLE());
                case Bool:
                    return (T) Boolean.valueOf(readByteOrEOF(input) == 1);
                case Enum8:
                    return (T) Byte.valueOf((byte) readUnsignedByte());
                case Enum16:
                    return (T) Short.valueOf((short) readUnsignedShortLE());
                case Date:
                    return convertDateTime(readDate(column.getTimeZone() == null ? timeZone :
                            column.getTimeZone()), typeHint);
                case Date32:
                    return convertDateTime(readDate32(column.getTimeZone() == null ? timeZone :
                            column.getTimeZone()), typeHint);
                case DateTime:
                    return convertDateTime(readDateTime32(column.getTimeZone() == null ? timeZone :
                            column.getTimeZone()), typeHint);
                case DateTime32:
                    return convertDateTime(readDateTime32(column.getTimeZone() == null ? timeZone :
                            column.getTimeZone()), typeHint);
                case DateTime64:
                    return convertDateTime(readDateTime64(column.getScale(), column.getTimeZone() == null ? timeZone :
                            column.getTimeZone()), typeHint);

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
                    return (T) readBigIntegerLE(8, true);
                case IPv4:
                    // https://clickhouse.com/docs/en/sql-reference/data-types/ipv4
                    return (T) Inet4Address.getByAddress(readNBytesLE(input, 4));
                case IPv6:
                    // https://clickhouse.com/docs/en/sql-reference/data-types/ipv6
                    return (T) Inet6Address.getByAddress(null, readNBytes(input, 16), null);
                case UUID:
                    return (T) new UUID(readLongLE(), readLongLE());
                case Point:
                    return (T) readGeoPoint();
                case Polygon:
                    return (T) readGeoPolygon();
                case MultiPolygon:
                    return (T) readGeoMultiPolygon();
                case Ring:
                    return (T) readGeoRing();

                case JSON: // experimental https://clickhouse.com/docs/en/sql-reference/data-types/newjson
                    if (jsonAsString) {
                        return (T) readString(input);
                    } else {
                        throw new RuntimeException("Reading JSON from binary is not implemented yet");
                    }
//                case Object: // deprecated https://clickhouse.com/docs/en/sql-reference/data-types/object-data-type
                case Array:
                    return convertArray(readArray(column), typeHint);
                case Map:
                    return (T) readMap(column);
//                case Nested:
                case Tuple:
                    return (T) readTuple(column);
                case Nothing:
                    return null;
                case SimpleAggregateFunction:
                    return (T) readValue(column.getNestedColumns().get(0));
                case AggregateFunction:
                    return (T) readBitmap( column);
                case Variant:
                    return (T) readVariant(column);
                default:
                    throw new IllegalArgumentException("Unsupported data type: " + column.getDataType());
            }
        } catch (EOFException e) {
            throw e;
        } catch (Exception e) {
            log.debug("Failed to read value for column {}, {}", column.getColumnName(), e.getLocalizedMessage());
            throw new ClientException("Failed to read value for column " + column.getColumnName(), e);
        }
    }

    private static <T> T convertDateTime(ZonedDateTime value, Class<?> typeHint) {
        if (typeHint == null) {
            return (T) value;
        }
        if (typeHint.isAssignableFrom(LocalDateTime.class)) {
            return (T) value.toLocalDateTime();
        } else if (typeHint.isAssignableFrom(LocalDate.class)) {
            return (T) value.toLocalDate();
        }

        return (T) value;
    }

    private static <T> T convertArray(ArrayValue value, Class<?> typeHint) {
        if (typeHint == null) {
            return (T) value;
        }
        if (typeHint.isAssignableFrom(List.class)) {
            return (T) value.asList();
        }
        if (typeHint.isArray()) {
            return (T) value.array;
        }

        return (T) value;
    }

    /**
     * Read a short value in little-endian from the internal input stream.
     *
     * @return short value
     * @throws IOException when IO error occurs
     */
    public short readShortLE() throws IOException {
        return readShortLE(input, bufferAllocator.allocate(INT16_SIZE));
    }

    /**
     * Reads a little-endian short from input stream. Uses buff to receive data from the input stream.
     *
     * @param input - source of bytes
     * @param buff  - buffer to store data
     * @return short value
     * @throws IOException when IO error occurs
     */
    public static short readShortLE(InputStream input, byte[] buff) throws IOException {
        readNBytes(input, buff, 0, 2);
        return (short) (buff[0] & 0xFF | (buff[1] & 0xFF) << 8);
    }

    /**
     * Reads an int value in little-endian from the internal input stream.
     * @return int value
     * @throws IOException when IO error occurs
     */
    public int readIntLE() throws IOException {
        return readIntLE(input, bufferAllocator.allocate(INT32_SIZE));
    }

    /**
     * Reads a little-endian int from input stream. Uses buff to receive data from the input stream.
     *
     * @param input - source of bytes
     * @param buff  - buffer to store data
     * @return - int value
     * @throws IOException when IO error occurs
     */
    public static int readIntLE(InputStream input, byte[] buff) throws IOException {
        readNBytes(input, buff, 0, 4);
        return (buff[0] & 0xFF) | (buff[1] & 0xFF) << 8 | (buff[2] & 0xFF) << 16 | (buff[3] & 0xFF) << 24;
    }

    /**
     * Reads a long value in little-endian from the internal input stream.
     *
     * @return long value
     * @throws IOException when IO error occurs
     */
    public long readLongLE() throws IOException {
        return readLongLE(input, bufferAllocator.allocate(INT64_SIZE));
    }

    /**
     * Reads a little-endian long from input stream. Uses buff to receive data from the input stream.
     *
     * @param input - source of bytes
     * @param buff  - buffer to store data
     * @return - long value
     * @throws IOException when IO error occurs
     */
    public static long readLongLE(InputStream input, byte[] buff) throws IOException {
        readNBytes(input, buff, 0, 8);
        return (long) (buff[0] & 0xFF) | (long) (buff[1] & 0xFF) << 8 | (long) (buff[2] & 0xFF) << 16
                | (long) (buff[3] & 0xFF) << 24 | (long) (buff[4] & 0xFF) << 32 | (long) (buff[5] & 0xFF) << 40
                | (long) (buff[6] & 0xFF) << 48 | (long) (buff[7] & 0xFF) << 56;
    }

    /**
     * Read byte from the internal input stream.
     * @return byte value
     * @throws IOException when IO error occurs
     */
    public byte readByte() throws IOException {
        return (byte) readByteOrEOF(input);
    }

    /**
     * Reads an unsigned byte value from the internal input stream.
     * @return unsigned byte value
     * @throws IOException when IO error occurs
     */
    public short readUnsignedByte() throws IOException {
        return (short) (readByteOrEOF(input) & 0xFF);
    }

    /**
     * Reads an unsigned short value from the internal input stream.
     * @return unsigned short value
     * @throws IOException when IO error occurs
     */
    public int readUnsignedShortLE() throws IOException {
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

    /**
     * Reads an unsigned int value in little-endian from the internal input stream.
     *
     * @return unsigned int value
     * @throws IOException when IO error occurs
     */
    public long readUnsignedIntLE() throws IOException {
        return readIntLE() & 0xFFFFFFFFL;
    }

    /**
     * Reads a little-endian unsigned int from input stream. Uses buff to receive data from the input stream.
     *
     * @param input - source of bytes
     * @param buff - buffer to store data
     * @return - unsigned int value
     * @throws IOException when IO error occurs
     */
    public static long readUnsignedIntLE(InputStream input, byte[] buff) throws IOException {
        return readIntLE(input, buff) & 0xFFFFFFFFL;
    }

    /**
     * Reads a big integer value in little-endian from the internal input stream.
     * @param len - number of bytes to read
     * @param unsigned - whether the value is unsigned
     * @return big integer value
     * @throws IOException when IO error occurs
     */
    public BigInteger readBigIntegerLE(int len, boolean unsigned) throws IOException {
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


    /**
     * Reads a decimal value from the internal input stream.
     * @return decimal value
     * @throws IOException when IO error occurs
     */
    public float readFloatLE() throws IOException {
        return Float.intBitsToFloat(readIntLE());
    }

    /**
     * Reads a double value from the internal input stream.
     * @return double value
     * @throws IOException when IO error occurs
     */
    public double readDoubleLE() throws IOException {
        return Double.longBitsToDouble(readLongLE());
    }

    /**
     * Reads a decimal value from the internal input stream.
     * @param precision - precision of the decimal value
     * @param scale - scale of the decimal value
     * @return decimal value
     * @throws IOException when IO error occurs
     */
    public BigDecimal readDecimal(int precision, int scale) throws IOException {
        BigDecimal v;

        if (precision <= ClickHouseDataType.Decimal32.getMaxScale()) {
            return BigDecimal.valueOf(readIntLE(), scale);
        } else if (precision <= ClickHouseDataType.Decimal64.getMaxScale()) {
            v = BigDecimal.valueOf(readLongLE(), scale);
        } else if (precision <= ClickHouseDataType.Decimal128.getMaxScale()) {
            v = new BigDecimal(readBigIntegerLE(INT128_SIZE, false), scale);
        } else {
            v = new BigDecimal(readBigIntegerLE(INT256_SIZE, false), scale);
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

    /**
     * Reads a array into an ArrayValue object.
     * @param column - column information
     * @return array value
     * @throws IOException when IO error occurs
     */
    public ArrayValue readArray(ClickHouseColumn column) throws IOException {
        int len = readVarInt(input);
        if (len == 0) {
            return new ArrayValue(Object.class, 0);
        }

        ArrayValue array;
        ClickHouseColumn itemTypeColumn = column.getNestedColumns().get(0);
        if (column.getArrayNestedLevel() == 1) {
            array = readArrayItem(itemTypeColumn, len);

        } else {
            array = new ArrayValue(ArrayValue.class, len);
            for (int i = 0; i < len; i++) {
                array.set(i, readArray(itemTypeColumn));
            }
        }

        return array;
    }

    public ArrayValue readArrayItem(ClickHouseColumn itemTypeColumn, int len) throws IOException {
        ArrayValue array;
        if (itemTypeColumn.isNullable()) {
            array = new ArrayValue(Object.class, len);
            for (int i = 0; i < len; i++) {
                array.set(i, readValue(itemTypeColumn));
            }
        } else {
            Object firstValue = readValue(itemTypeColumn);
            Class<?> itemClass = firstValue.getClass();
            if (firstValue instanceof Byte) {
                itemClass = byte.class;
            } else if (firstValue instanceof Character) {
                itemClass = char.class;
            } else if (firstValue instanceof Short) {
                itemClass = short.class;
            } else if (firstValue instanceof Integer) {
                itemClass = int.class;
            } else if (firstValue instanceof Long) {
                itemClass = long.class;
            } else if (firstValue instanceof Boolean) {
                itemClass = boolean.class;
            }

            array = new ArrayValue(itemClass, len);
            array.set(0, firstValue);
            for (int i = 1; i < len; i++) {
                array.set(i, readValue(itemTypeColumn));
            }
        }
        return array;
    }

    public void skipValue(ClickHouseColumn column) throws IOException {
        readValue(column, null);
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
                    array = Array.newInstance(Object[].class, length);
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

    /**
     * Reads a map.
     * @param column - column information
     * @return a map
     * @throws IOException when IO error occurs
     */
    public Map<?, ?> readMap(ClickHouseColumn column) throws IOException {
        int len = readVarInt(input);
        if (len == 0) {
            return Collections.emptyMap();
        }

        ClickHouseColumn keyType = column.getKeyInfo();
        ClickHouseColumn valueType = column.getValueInfo();
        LinkedHashMap<Object, Object> map = new LinkedHashMap<>(len);
        for (int i = 0; i < len; i++) {
            Object key = readValue(keyType);
            Object value = readValue(valueType);
            map.put(key, value);
        }
        return map;
    }

    /**
     * Reads a tuple.
     * @param column - column information
     * @return a tuple
     * @throws IOException when IO error occurs
     */
    public Object[] readTuple(ClickHouseColumn column) throws IOException {
        int len = column.getNestedColumns().size();
        Object[] tuple = new Object[len];
        for (int i = 0; i < len; i++) {
            tuple[i] = readValue(column.getNestedColumns().get(i));
        }

        return tuple;
    }

    public Object readVariant(ClickHouseColumn column) throws IOException {
        int ordNum = readByte();
        return readValue(column.getNestedColumns().get(ordNum));
    }

    /**
     * Reads a GEO point as an array of two doubles what represents coordinates (X, Y).
     * @return X, Y coordinates
     * @throws IOException when IO error occurs
     */
    public double[] readGeoPoint() throws IOException {
        return new double[]{readDoubleLE(), readDoubleLE()};
    }

    /**
     * Reads a GEO ring as an array of points.
     * @return array of points
     * @throws IOException when IO error occurs
     */
    public double[][] readGeoRing() throws IOException {
        int count = readVarInt(input);
        double[][] value = new double[count][2];
        for (int i = 0; i < count; i++) {
            value[i] = readGeoPoint();
        }
        return value;
    }


    /**
     * Reads a GEO polygon as an array of rings.
     * @return polygon
     * @throws IOException when IO error occurs
     */
    public double[][][] readGeoPolygon() throws IOException {
        int count = readVarInt(input);
        double[][][] value = new double[count][][];
        for (int i = 0; i < count; i++) {
            value[i] = readGeoRing();
        }
        return value;
    }

    /**
     * Reads a GEO multipolygon as an array of polygons.
     * @return multipolygon
     * @throws IOException when IO error occurs
     */
    private double[][][][] readGeoMultiPolygon() throws IOException {
        int count = readVarInt(input);
        double[][][][] value = new double[count][][][];
        for (int i = 0; i < count; i++) {
            value[i] = readGeoPolygon();
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

    /**
     * Reads a Date value from internal input stream.
     * @param tz - timezone
     * @return ZonedDateTime
     * @throws IOException when IO error occurs
     */
    private ZonedDateTime readDate(TimeZone tz) throws IOException {
        return readDate(input, bufferAllocator.allocate(INT16_SIZE), tz);
    }

    /**
     * Reads a date from input stream.
     * @param input - source of bytes
     * @param buff - for reading short value. Should be 2 bytes.
     * @param tz - timezone
     * @return ZonedDateTime
     * @throws IOException when IO error occurs
     */
    public static ZonedDateTime readDate(InputStream input, byte[] buff, TimeZone tz) throws IOException {
        LocalDate d = LocalDate.ofEpochDay(readUnsignedShortLE(input, buff));
        return d.atStartOfDay(tz.toZoneId()).withZoneSameInstant(tz.toZoneId());
    }

    /**
     * Reads a Date32 value from internal input stream.
     * @param tz - timezone
     * @return ZonedDateTime
     * @throws IOException when IO error occurs
     */
    public ZonedDateTime readDate32(TimeZone tz)
            throws IOException {
        return readDate32(input, bufferAllocator.allocate(INT32_SIZE), tz);
    }

    /**
     * Reads a date32 from input stream.
     *
     * @param input - source of bytes
     * @param buff - for reading int value. Should be 4 bytes.
     * @param tz - timezone
     * @return ZonedDateTime
     * @throws IOException when IO error occurs
     */
    public static ZonedDateTime readDate32(InputStream input, byte[] buff, TimeZone tz)
            throws IOException {
        LocalDate d = LocalDate.ofEpochDay(readIntLE(input, buff));
        return d.atStartOfDay(tz.toZoneId()).withZoneSameInstant(tz.toZoneId());
    }

    private ZonedDateTime readDateTime32(TimeZone tz) throws IOException {
        return readDateTime32(input, bufferAllocator.allocate(INT32_SIZE), tz);
    }

    /**
     * Reads a datetime32 from input stream.
     * @param input - source of bytes
     * @param buff - for reading int value. Should be 4 bytes.
     * @param tz - timezone
     * @return ZonedDateTime
     * @throws IOException when IO error occurs
     */
    public static ZonedDateTime readDateTime32(InputStream input, byte[] buff, TimeZone tz) throws IOException {
        long time = readUnsignedIntLE(input, buff);
        return LocalDateTime.ofInstant(Instant.ofEpochSecond(Math.max(time, 0L)), tz.toZoneId()).atZone(tz.toZoneId());
    }

    /**
     * Reads a datetime64 from internal input stream.
     * @param scale - scale of the datetime64
     * @param tz - timezone
     * @return ZonedDateTime
     * @throws IOException when IO error occurs
     */
    public ZonedDateTime readDateTime64(int scale, TimeZone tz) throws IOException {
        return readDateTime64(input, bufferAllocator.allocate(INT64_SIZE), scale, tz);
    }


    /**
     * Bases for datetime64.
     */
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

    /**
     * Reads a decimal value from input stream.
     * @param input - source of bytes
     * @return String
     * @throws IOException when IO error occurs
     */
    public static String readString(InputStream input) throws IOException {
        int len = readVarInt(input);
        if (len == 0) {
            return "";
        }
        return new String(readNBytes(input, len), StandardCharsets.UTF_8);
    }

    public static int readByteOrEOF(InputStream input) throws IOException {
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

    public static boolean isReadToPrimitive(ClickHouseDataType dataType) {
        switch (dataType) {
            case Int8:
            case UInt8:
            case Int16:
            case UInt16:
            case Int32:
            case UInt32:
            case Int64:
            case Float32:
            case Float64:
            case Bool:
            case Enum8:
            case Enum16:
                return true;
            default:
                return false;
        }
    }

    private ClickHouseBitmap readBitmap(ClickHouseColumn column) throws IOException {
        return ClickHouseBitmap.deserialize(input, column.getNestedColumns().get(0).getDataType());
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

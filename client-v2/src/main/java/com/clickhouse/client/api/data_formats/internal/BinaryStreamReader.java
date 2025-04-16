package com.clickhouse.client.api.data_formats.internal;

import com.clickhouse.client.api.ClientException;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseEnum;
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
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

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

        ClickHouseColumn actualColumn = column.getDataType() == ClickHouseDataType.Dynamic ? readDynamicData() : column;
        ClickHouseDataType dataType = actualColumn.getDataType();
        int precision = actualColumn.getPrecision();
        int scale = actualColumn.getScale();
        TimeZone timezone = actualColumn.getTimeZoneOrDefault(timeZone);

        try {
            switch (dataType) {
                // Primitives
                case FixedString: {
                    byte[] bytes = precision > STRING_BUFF.length ?
                            new byte[precision] : STRING_BUFF;
                    readNBytes(input, bytes, 0, precision);
                    return (T) new String(bytes, 0, precision, StandardCharsets.UTF_8);
                }
                case String: {
                    return (T) readString();
                }
                case Int8:
                    return (T) Byte.valueOf(readByte());
                case UInt8:
                    return (T) Short.valueOf(readUnsignedByte());
                case Int16:
                    return (T) (Short)readShortLE();
                case UInt16:
                    return (T) (Integer)readUnsignedShortLE();
                case Int32:
                    return (T) (Integer)readIntLE();
                case UInt32:
                    return (T) (Long)(readUnsignedIntLE());
                case Int64:
                    return (T) (Long)(readLongLE());
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
                    return (T) readDecimal(precision, scale);
                case Decimal32:
                    return (T) readDecimal(ClickHouseDataType.Decimal32.getMaxPrecision(), scale);
                case Decimal64:
                    return (T) readDecimal(ClickHouseDataType.Decimal64.getMaxPrecision(), scale);
                case Decimal128:
                    return (T) readDecimal(ClickHouseDataType.Decimal128.getMaxPrecision(), scale);
                case Decimal256:
                    return (T) readDecimal(ClickHouseDataType.Decimal256.getMaxPrecision(), scale);
                case Float32:
                    return (T) (Float)readFloatLE();
                case Float64:
                    return (T) (Double)readDoubleLE();
                case Bool:
                    return (T) Boolean.valueOf(readByteOrEOF(input) == 1);
                case Enum8: {
                    byte enum8Val = (byte) readUnsignedByte();
                    String name = actualColumn.getEnumConstants().nameNullable(enum8Val);
                    return (T) new EnumValue(name == null ? "<unknown>" : name, enum8Val);
                }
                case Enum16: {
                    short enum16Val = (short) readUnsignedShortLE();
                    String name = actualColumn.getEnumConstants().nameNullable(enum16Val);
                    return (T) new EnumValue(name == null ? "<unknown>" : name, enum16Val);
                }
                case Date:
                    return convertDateTime(readDate(timezone), typeHint);
                case Date32:
                    return convertDateTime(readDate32(timezone), typeHint);
                case DateTime:
                    return convertDateTime(readDateTime32(timezone), typeHint);
                case DateTime32:
                    return convertDateTime(readDateTime32(timezone), typeHint);
                case DateTime64:
                    return convertDateTime(readDateTime64(scale, timezone), typeHint);
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
                    return (T) readIntervalValue(dataType, input);
                case IPv4:
                    // https://clickhouse.com/docs/en/sql-reference/data-types/ipv4
                    return (T) Inet4Address.getByAddress(readNBytesLE(input, 4));
                case IPv6:
                    // https://clickhouse.com/docs/en/sql-reference/data-types/ipv6
                    return (T) Inet6Address.getByAddress(readNBytes(input, 16));
                case UUID:
                    return (T) new UUID(readLongLE(), readLongLE());
                case Point:
                    return (T) readGeoPoint();
                case Polygon:
                    return (T) readGeoPolygon();
                case MultiPolygon:
                    return (T) readGeoMultiPolygon();
                case MultiLineString:
                    return (T) readGeoPolygon();
                case Ring:
                    return (T) readGeoRing();
                case LineString:
                    return (T) readGeoRing();
                case JSON: // experimental https://clickhouse.com/docs/en/sql-reference/data-types/newjson
                    if (jsonAsString) {
                        return (T) readString(input);
                    } else {
                        return (T) readJsonData(input);
                    }
//                case Object: // deprecated https://clickhouse.com/docs/en/sql-reference/data-types/object-data-type
                case Array:
                    return convertArray(readArray(actualColumn), typeHint);
                case Map:
                    return (T) readMap(actualColumn);
                case Tuple:
                    return (T) readTuple(actualColumn);
                case Nothing:
                    return null;
                case SimpleAggregateFunction:
                    return (T) readValue(column.getNestedColumns().get(0));
                case AggregateFunction:
                    return (T) readBitmap( actualColumn);
                case Variant:
                    return (T) readVariant(actualColumn);
                case Dynamic:
                    return (T) readValue(actualColumn, typeHint);
                default:
                    throw new IllegalArgumentException("Unsupported data type: " + actualColumn.getDataType());
            }
        } catch (EOFException e) {
            throw e;
        } catch (Exception e) {
            log.debug("Failed to read value for column {}, {}", column.getColumnName(), e.getLocalizedMessage());
            throw new ClientException("Failed to read value for column " + column.getColumnName(), e);
        }
    }

    private TemporalAmount readIntervalValue(ClickHouseDataType dataType, InputStream input) throws IOException {
        BigInteger v = readBigIntegerLE(8, true);

        switch (dataType) {
            case IntervalYear:
                return Period.ofYears(v.intValue());
            case IntervalQuarter:
                return Period.ofMonths(3 * v.intValue());
            case IntervalMonth:
                return Period.ofMonths(v.intValue());
            case IntervalWeek:
                return Period.ofWeeks(v.intValue());
            case IntervalDay:
                return Period.ofDays(v.intValue());
            case IntervalHour:
                return Duration.ofHours(v.longValue());
            case IntervalMinute:
                return Duration.ofMinutes(v.longValue());
            case IntervalSecond:
                return Duration.ofSeconds(v.longValue());
            case IntervalMicrosecond:
                return Duration.ofNanos(v.longValue() * 1000);
            case IntervalMillisecond:
                return Duration.ofMillis(v.longValue());
            case IntervalNanosecond:
                return Duration.ofNanos(v.longValue());
            default:
                throw new ClientException("Unsupported interval type: " + dataType);
        }
    }

    private static <T> T convertDateTime(ZonedDateTime value, Class<?> typeHint) {
        if (typeHint == null) {
            return (T) value;
        }
        if (LocalDateTime.class.isAssignableFrom(typeHint)) {
            return (T) value.toLocalDateTime();
        } else if (LocalDate.class.isAssignableFrom(typeHint)) {
            return (T) value.toLocalDate();
        }

        return (T) value;
    }

    private static <T> T convertArray(ArrayValue value, Class<?> typeHint) {
        if (typeHint == null) {
            return (T) value;
        }
        if (typeHint == Object.class) {
            return (T) value.asList();
        }
        if (List.class.isAssignableFrom(typeHint)) {
            return (T) value.asList();
        }
        if (typeHint.isArray()) {
            return (T) value.array;
        }

        return (T) value;
    }


    private byte[] int16Buff = new byte[INT16_SIZE];

    /**
     * Read a short value in little-endian from the internal input stream.
     *
     * @return short value
     * @throws IOException when IO error occurs
     */
    public short readShortLE() throws IOException {
        return readShortLE(input, int16Buff);
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

    private byte[] int32Buff = new byte[INT32_SIZE];

    /**
     * Reads an int value in little-endian from the internal input stream.
     * @return int value
     * @throws IOException when IO error occurs
     */
    public int readIntLE() throws IOException {
        return readIntLE(input, int32Buff);
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

    private byte[] int64Buff = new byte[INT64_SIZE];

    /**
     * Reads a long value in little-endian from the internal input stream.
     *
     * @return long value
     * @throws IOException when IO error occurs
     */
    public long readLongLE() throws IOException {
        return readLongLE(input, int64Buff);
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

        return (0xFFL & buff[0]) | ((0xFFL & buff[1]) << 8) | ((0xFFL & buff[2]) << 16)
                | ((0xFFL & buff[3]) << 24) | ((0xFFL & buff[4]) << 32)
                | ((0xFFL & buff[5]) << 40) | ((0xFFL & buff[6]) << 48)
                | ((0xFFL & buff[7]) << 56);
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
        return readUnsignedShortLE(input, int16Buff);
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

    private static final byte[] B1 = new byte[8];
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

        int nextPos = 0;

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

        public boolean append(Object value) {
            set(nextPos++, value);
            return nextPos == length;
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

    public static class EnumValue extends Number {

        public final String name;

        public final int value;

        public EnumValue(String name, int value) {
            this.name = name;
            this.value = value;
        }

        @Override
        public int intValue() {
            return value;
        }

        @Override
        public long longValue() {
            return value;
        }

        @Override
        public float floatValue() {
            return value;
        }

        @Override
        public double doubleValue() {
            return value;
        }

        @Override
        public String toString() {
            return name;
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

        return Instant.ofEpochSecond(value, nanoSeconds).atZone(tz.toZoneId());
    }

    private final byte[] STRING_BUFF = new byte[1024];

    /**
     * Reads a string from the internal input stream.
     * Uses pre-allocated buffer to store tmp data.
     * @return
     * @throws IOException
     */
    public String readString() throws IOException {
        int len = readVarInt(input);
        if (len == 0) {
            return "";
        }
        byte[] dest = len > STRING_BUFF.length ? new byte[len] : STRING_BUFF;
        readNBytes(input, dest, 0, len);
        return new String(dest, 0, len, StandardCharsets.UTF_8);
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

    private static final Set<Byte> DECIMAL_TAGS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            ClickHouseDataType.Decimal.getBinTag(),
            ClickHouseDataType.Decimal32.getBinTag(),
            ClickHouseDataType.Decimal64.getBinTag(),
            ClickHouseDataType.Decimal128.getBinTag(),
            ClickHouseDataType.Decimal256.getBinTag()
    )));

    private ClickHouseColumn readDynamicData() throws IOException {
        byte tag = readByte();

        ClickHouseDataType type;
        if (tag == ClickHouseDataType.INTERVAL_BIN_TAG) {
            byte intervalKind = readByte();
            type = ClickHouseDataType.intervalKind2Type.get(intervalKind);
            if (type == null) {
                throw new ClientException("Unsupported interval kind: " + intervalKind);
            }
            return ClickHouseColumn.of("v", type, false, 0, 0);
        } else if (tag == ClickHouseDataType.DateTime32.getBinTag()) {
            byte scale = readByte();
            return ClickHouseColumn.of("v", "DateTime32(" + scale + ")");
        }  else if (tag == ClickHouseDataType.DateTime64.getBinTag() - 1) { // without timezone
            byte scale = readByte();
            return ClickHouseColumn.of("v", "DateTime64(" + scale  +")");
        }  else if (tag == ClickHouseDataType.DateTime64.getBinTag()) {
            byte scale = readByte();
            String timezone = readString(input);
            return ClickHouseColumn.of("v", "DateTime64(" + scale + (timezone.isEmpty() ? "" : ", " + timezone) +")");
        } else if (tag == ClickHouseDataType.CUSTOM_TYPE_BIN_TAG) {
            String typeName = readString(input);
            return ClickHouseColumn.of("v", typeName);
        } else if (DECIMAL_TAGS.contains(tag)) {
            int precision = readByte();
            int scale = readByte();
            return ClickHouseColumn.of("v", ClickHouseDataType.binTag2Type.get(tag), false, precision, scale);
        } else if (tag == ClickHouseDataType.Array.getBinTag()) {
            ClickHouseColumn elementColumn = readDynamicData();
            return ClickHouseColumn.of("v", "Array(" + elementColumn.getOriginalTypeName() + ")");
        } else if (tag == ClickHouseDataType.Map.getBinTag()) {
            ClickHouseColumn keyInfo = readDynamicData();
            ClickHouseColumn valueInfo = readDynamicData();
            return ClickHouseColumn.of("v", "Map(" + keyInfo.getOriginalTypeName() + "," + valueInfo.getOriginalTypeName() + ")");
        } else if (tag == ClickHouseDataType.Enum8.getBinTag() || tag == ClickHouseDataType.Enum16.getBinTag()) {
            int constants = readVarInt(input);
            int[] values = new int[constants];
            String[] names = new String[constants];
            ClickHouseDataType enumType = constants > 127 ? ClickHouseDataType.Enum16 : ClickHouseDataType.Enum8;
            for (int i = 0; i < constants; i++) {
                names[i] = readString(input);
                if (enumType == ClickHouseDataType.Enum8) {
                    values[i] = readUnsignedByte();
                } else {
                    values[i] = readUnsignedShortLE();
                }
            }
            return new ClickHouseColumn(enumType, "v", enumType.name(), false, false, Collections.emptyList(), Collections.emptyList(),
                    new ClickHouseEnum(names, values));
        } else if (tag == ClickHouseDataType.NULLABLE_BIN_TAG) {
            ClickHouseColumn column = readDynamicData();
            return ClickHouseColumn.of("v", "Nullable(" + column.getOriginalTypeName() + ")");
        } else {
            type = ClickHouseDataType.binTag2Type.get(tag);
            if (type == null) {
                throw new ClientException("Unsupported data type with tag " + tag);
            }
            return ClickHouseColumn.of("v", type, false, 0, 0);
        }
    }

    private static final ClickHouseColumn JSON_PLACEHOLDER_COL = ClickHouseColumn.parse("v Dynamic").get(0);

    private Map<String, Object> readJsonData(InputStream input) throws IOException {
        int numOfPaths = readVarInt(input);
        if (numOfPaths == 0) {
            return Collections.emptyMap();
        }

        Map<String, Object> obj = new HashMap<>();
        for (int i = 0; i < numOfPaths; i++) {
            String path = readString(input);
            Object value = readValue(JSON_PLACEHOLDER_COL);
            obj.put(path, value);
        }
        return obj;
    }
}

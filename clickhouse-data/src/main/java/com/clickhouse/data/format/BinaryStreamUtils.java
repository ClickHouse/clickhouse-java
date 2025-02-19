package com.clickhouse.data.format;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.clickhouse.data.ClickHouseByteBuffer;
import com.clickhouse.data.ClickHouseByteUtils;
import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseUtils;
import com.clickhouse.data.ClickHouseValues;
import com.clickhouse.data.value.ClickHouseBitmap;

/**
 * Utility class for dealing with binary stream and data.
 */

@Deprecated
public final class BinaryStreamUtils {
    public static final int U_INT8_MAX = (1 << 8) - 1;
    public static final int U_INT16_MAX = (1 << 16) - 1;
    public static final long U_INT32_MAX = (1L << 32) - 1;
    public static final BigInteger U_INT64_MAX = new BigInteger(1, new byte[] { (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
            (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF });
    public static final BigInteger U_INT128_MAX = new BigInteger(1,
            new byte[] { (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                    (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                    (byte) 0xFF, (byte) 0xFF });
    public static final BigInteger U_INT256_MAX = new BigInteger(1,
            new byte[] { (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                    (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                    (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                    (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                    (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF });

    public static final int DATE32_MAX = (int) LocalDate.of(2299, 12, 31).toEpochDay();
    public static final int DATE32_MIN = (int) LocalDate.of(1900, 1, 1).toEpochDay();

    public static final BigDecimal DECIMAL32_MAX = new BigDecimal("1000000000");
    public static final BigDecimal DECIMAL32_MIN = new BigDecimal("-1000000000");

    public static final BigDecimal DECIMAL64_MAX = new BigDecimal("1000000000000000000");
    public static final BigDecimal DECIMAL64_MIN = new BigDecimal("-1000000000000000000");

    public static final BigDecimal DECIMAL128_MAX = new BigDecimal("100000000000000000000000000000000000000");
    public static final BigDecimal DECIMAL128_MIN = new BigDecimal("-100000000000000000000000000000000000000");

    public static final BigDecimal DECIMAL256_MAX = new BigDecimal(
            "10000000000000000000000000000000000000000000000000000000000000000000000000000");
    public static final BigDecimal DECIMAL256_MIN = new BigDecimal(
            "-10000000000000000000000000000000000000000000000000000000000000000000000000000");

    public static final long DATETIME64_MAX = LocalDateTime.of(LocalDate.of(2299, 12, 31), LocalTime.MAX)
            .toEpochSecond(ZoneOffset.UTC);
    public static final long DATETIME64_9_MAX = LocalDateTime.of(2262, 4, 11, 23, 47, 16, 0)
            .toEpochSecond(ZoneOffset.UTC);
    public static final long DATETIME64_MIN = LocalDateTime.of(LocalDate.of(1900, 1, 1), LocalTime.MIN)
            .toEpochSecond(ZoneOffset.UTC);

    public static final long MILLIS_IN_DAY = TimeUnit.DAYS.toMillis(1);

    public static final long DATETIME_MAX = U_INT32_MAX * 1000L;

    public static final BigDecimal NANOS = new BigDecimal(BigInteger.TEN.pow(9));

    private static final int[] BASES = new int[] { 1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000,
            1000000000 };

    private static <T extends Enum<T>> T toEnum(int value, Class<T> enumType) {
        for (T t : ClickHouseChecker.nonNull(enumType, "enumType").getEnumConstants()) {
            if (t.ordinal() == value) {
                return t;
            }
        }

        throw new IllegalArgumentException(
                ClickHouseUtils.format("Enum [%s] does not contain value [%d]", enumType, value));
    }

    public static int toInt32(ClickHouseByteBuffer byteBuffer) {
        return toInt32(byteBuffer.array(), byteBuffer.position());
    }

    public static int toInt32(byte[] bytes, int offset) {
        return ClickHouseByteUtils.getInt32(bytes, offset);
    }

    public static long toInt64(ClickHouseByteBuffer byteBuffer) {
        return toInt64(byteBuffer.array(), byteBuffer.position());
    }

    public static long toInt64(byte[] bytes, int offset) {
        return ClickHouseByteUtils.getInt64(bytes, offset);
    }

    public static void setInt32(byte[] bytes, int offset, int value) {
        ClickHouseByteUtils.setInt32(bytes, offset, value);
    }

    public static void setInt64(byte[] bytes, int offset, long value) {
        ClickHouseByteUtils.setInt64(bytes, offset, value);
    }

    /**
     * Reverse the given byte array.
     * 
     * @param bytes byte array to manipulate
     * @return same byte array but reserved
     */
    public static byte[] reverse(byte[] bytes) {
        int l = bytes != null ? bytes.length : 0;
        if (l > 1) {
            for (int i = 0, len = l / 2; i < len; i++) {
                byte b = bytes[i];
                --l;
                bytes[i] = bytes[l];
                bytes[l] = b;
            }
        }

        return bytes;
    }

    /**
     * Get varint length of given integer.
     *
     * @param value integer
     * @return varint length
     */
    public static int getVarIntSize(int value) {
        int result = 0;
        do {
            result++;
            value >>>= 7;
        } while (value != 0);

        return result;
    }

    /**
     * Get varint length of given long.
     *
     * @param value long
     * @return varint length
     */
    public static int getVarLongSize(long value) {
        int result = 0;
        do {
            result++;
            value >>>= 7;
        } while (value != 0);

        return result;
    }

    /**
     * Writes bytes into given output stream.
     *
     * @param output non-null output stream
     * @param buffer non-null byte buffer
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    @SuppressWarnings("squid:S2095")
    public static void writeByteBuffer(OutputStream output, ByteBuffer buffer) throws IOException {
        Channels.newChannel(output).write(buffer);
    }

    /**
     * Reads bitmap from given input stream. It behaves in a similar way as
     * {@link java.io.DataInput#readFully(byte[])}.
     *
     * @param input    non-null input
     * @param dataType number of characters to read
     * @return non-null bitmap wrapper class
     * @throws IOException when failed to read value from input stream, not able to
     *                     retrieve all bytes, or reached end of the stream
     */
    public static ClickHouseBitmap readBitmap(ClickHouseInputStream input, ClickHouseDataType dataType)
            throws IOException {
        return ClickHouseBitmap.deserialize(input, dataType);
    }

    /**
     * Writes bitmap into given output stream.
     *
     * @param output non-null output stream
     * @param bitmap non-null bitmap
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeBitmap(OutputStream output, ClickHouseBitmap bitmap) throws IOException {
        writeByteBuffer(output, bitmap.toByteBuffer());
    }

    /**
     * Writes bytes into given output stream.
     *
     * @param output non-null output stream
     * @param bytes  non-null byte array
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeBytes(OutputStream output, byte[] bytes) throws IOException {
        output.write(bytes);
    }

    /**
     * Reads {@code length} characters from given reader. It behaves in a similar
     * way as {@link java.io.DataInput#readFully(byte[])}.
     *
     * @param input  non-null reader
     * @param length number of characters to read
     * @return character array and its length should be {@code length}
     * @throws IOException when failed to read value from input stream, not able to
     *                     retrieve all bytes, or reached end of the stream
     */
    public static char[] readCharacters(Reader input, int length) throws IOException {
        int count = 0;
        char[] chars = new char[length];
        while (count < length) {
            int n = input.read(chars, count, length - count);
            if (n < 0) {
                try {
                    input.close();
                } catch (IOException e) {
                    // ignore
                }

                throw count == 0 ? new EOFException()
                        : new IOException(ClickHouseUtils
                                .format("Reached end of reader after reading %d of %d characters", count, length));
            }
            count += n;
        }

        return chars;
    }

    /**
     * Read boolean from given input stream. It uses
     * {@link ClickHouseInputStream#readByte()}
     * to get value and return {@code true} only when the value is {@code 1}.
     *
     * @param input non-null input stream
     * @return boolean
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static boolean readBoolean(ClickHouseInputStream input) throws IOException {
        return ClickHouseChecker.between(input.readByte(), ClickHouseValues.TYPE_BOOLEAN, 0, 1) == 1;
    }

    /**
     * Write boolean into given output stream.
     *
     * @param output non-null output stream
     * @param value  boolean value, true == 1 and false == 0
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeBoolean(OutputStream output, boolean value) throws IOException {
        output.write(value ? 1 : 0);
    }

    /**
     * Write integer as boolean into given output stream.
     *
     * @param output non-null output stream
     * @param value  integer, everyting else besides one will be treated as
     *               zero(false)
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeBoolean(OutputStream output, int value) throws IOException {
        output.write(ClickHouseChecker.between(value, ClickHouseValues.TYPE_INT, 0, 1) == 1 ? 1 : 0);
    }

    /**
     * Read enum value from given input stream.
     *
     * @param <T>      enum type
     * @param input    non-null input stream
     * @param enumType enum class
     * @return enum value
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static <T extends Enum<T>> T readEnum8(ClickHouseInputStream input, Class<T> enumType) throws IOException {
        return toEnum(readEnum8(input), enumType);
    }

    /**
     * Read enum value from given input stream. Same as
     * {@link #readInt8(ClickHouseInputStream)}.
     *
     * @param input non-null input stream
     * @return enum value
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static byte readEnum8(ClickHouseInputStream input) throws IOException {
        return readInt8(input);
    }

    /**
     * Write enum value into given output stream. Same as
     * {@link #writeInt8(OutputStream, byte)}.
     *
     * @param output non-null output stream
     * @param value  enum value
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeEnum8(OutputStream output, byte value) throws IOException {
        writeInt8(output, value);
    }

    /**
     * Write enum value into given output stream.
     *
     * @param <T>    type of the value
     * @param output non-null output stream
     * @param value  enum value
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static <T extends Enum<T>> void writeEnum8(OutputStream output, T value) throws IOException {
        writeEnum8(output, (byte) ClickHouseChecker.nonNull(value, "enum value").ordinal());
    }

    /**
     * Read enum value from given input stream.
     *
     * @param <T>      enum type
     * @param input    non-null input stream
     * @param enumType enum class
     * @return enum value
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static <T extends Enum<T>> T readEnum16(ClickHouseInputStream input, Class<T> enumType) throws IOException {
        return toEnum(readEnum16(input), enumType);
    }

    /**
     * Read enum value from given input stream. Same as
     * {@link #readInt16(ClickHouseInputStream)}.
     *
     * @param input non-null input stream
     * @return enum value
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static short readEnum16(ClickHouseInputStream input) throws IOException {
        return readInt16(input);
    }

    /**
     * Write enum value into given output stream. Same as
     * {@link #writeInt16(OutputStream, int)}.
     *
     * @param output non-null output stream
     * @param value  enum value
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeEnum16(OutputStream output, int value) throws IOException {
        writeInt16(output, value);
    }

    /**
     * Write enum value into given output stream.
     *
     * @param <T>    type of the value
     * @param output non-null output stream
     * @param value  enum value
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static <T extends Enum<T>> void writeEnum16(OutputStream output, T value) throws IOException {
        writeEnum16(output, ClickHouseChecker.nonNull(value, "enum value").ordinal());
    }

    /**
     * Read geo point(X and Y coordinates) from given input stream.
     *
     * @param input non-null input stream
     * @return X and Y coordinates
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static double[] readGeoPoint(ClickHouseInputStream input) throws IOException {
        return new double[] { readFloat64(input), readFloat64(input) };
    }

    /**
     * Write geo point(X and Y coordinates).
     *
     * @param output non-null output stream
     * @param value  X and Y coordinates
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeGeoPoint(OutputStream output, double[] value) throws IOException {
        if (value == null || value.length != 2) {
            throw new IllegalArgumentException("Non-null X and Y coordinates are required");
        }

        writeGeoPoint(output, value[0], value[1]);
    }

    /**
     * Write geo point(X and Y coordinates).
     *
     * @param output non-null output stream
     * @param x      X coordinate
     * @param y      Y coordinate
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeGeoPoint(OutputStream output, double x, double y) throws IOException {
        writeFloat64(output, x);
        writeFloat64(output, y);
    }

    /**
     * Read geo ring(array of X and Y coordinates) from given input stream.
     *
     * @param input non-null input stream
     * @return array of X and Y coordinates
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static double[][] readGeoRing(ClickHouseInputStream input) throws IOException {
        int count = readVarInt(input);
        double[][] value = new double[count][2];
        for (int i = 0; i < count; i++) {
            value[i] = readGeoPoint(input);
        }
        return value;
    }

    /**
     * Write geo ring(array of X and Y coordinates).
     *
     * @param output non-null output stream
     * @param value  array of X and Y coordinates
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeGeoRing(OutputStream output, double[][] value) throws IOException {
        writeVarInt(output, value.length);
        for (double[] v : value) {
            writeGeoPoint(output, v);
        }
    }

    /**
     * Read geo polygon(array of rings) from given input stream.
     *
     * @param input non-null input stream
     * @return array of rings
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static double[][][] readGeoPolygon(ClickHouseInputStream input) throws IOException {
        int count = readVarInt(input);
        double[][][] value = new double[count][][];
        for (int i = 0; i < count; i++) {
            value[i] = readGeoRing(input);
        }
        return value;
    }

    /**
     * Write geo polygon(array of rings).
     *
     * @param output non-null output stream
     * @param value  array of rings
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeGeoPolygon(OutputStream output, double[][][] value) throws IOException {
        writeVarInt(output, value.length);
        for (double[][] v : value) {
            writeGeoRing(output, v);
        }
    }

    /**
     * Read geo multi-polygon(array of polygons) from given input stream.
     *
     * @param input non-null input stream
     * @return array of polygons
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static double[][][][] readGeoMultiPolygon(ClickHouseInputStream input) throws IOException {
        int count = readVarInt(input);
        double[][][][] value = new double[count][][][];
        for (int i = 0; i < count; i++) {
            value[i] = readGeoPolygon(input);
        }
        return value;
    }

    /**
     * Write geo polygon(array of rings).
     *
     * @param output non-null output stream
     * @param value  array of polygons
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeGeoMultiPolygon(OutputStream output, double[][][][] value) throws IOException {
        writeVarInt(output, value.length);
        for (double[][][] v : value) {
            writeGeoPolygon(output, v);
        }
    }

    /**
     * Read null marker from input stream. Same as
     * {@link #readBoolean(ClickHouseInputStream)}.
     * 
     * @param input non-null input stream
     * @return true if it's null; false otherwise
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static boolean readNull(ClickHouseInputStream input) throws IOException {
        return readBoolean(input);
    }

    /**
     * Write null marker. Same as {@code writeBoolean(outut, true)}.
     *
     * @param output non-null output stream
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeNull(OutputStream output) throws IOException {
        writeBoolean(output, true);
    }

    /**
     * Write non-null marker. Same as {@code writeBoolean(outut, false)}.
     *
     * @param output non-null output stream
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeNonNull(OutputStream output) throws IOException {
        writeBoolean(output, false);
    }

    /**
     * Read Inet4Address from given input stream.
     *
     * @param input non-null input stream
     * @return Inet4Address
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static Inet4Address readInet4Address(ClickHouseInputStream input) throws IOException {
        return (Inet4Address) InetAddress.getByAddress(reverse(input.readBytes(4)));
    }

    /**
     * Write Inet4Address to given output stream.
     *
     * @param output non-null output stream
     * @param value  Inet4Address
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeInet4Address(OutputStream output, Inet4Address value) throws IOException {
        output.write(reverse(value.getAddress()));
    }

    /**
     * Read Inet6Address from given input stream.
     *
     * @param input non-null input stream
     * @return Inet6Address
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static Inet6Address readInet6Address(ClickHouseInputStream input) throws IOException {
        return Inet6Address.getByAddress(null, input.readBytes(16), null);
    }

    /**
     * Write Inet6Address to given output stream.
     *
     * @param output non-null output stream
     * @param value  Inet6Address
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeInet6Address(OutputStream output, Inet6Address value) throws IOException {
        output.write(value.getAddress());
    }

    /**
     * Read a byte from given input stream. Same as
     * {@link ClickHouseInputStream#readByte()}.
     *
     * @param input non-null input stream
     * @return byte
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static byte readInt8(ClickHouseInputStream input) throws IOException {
        return input.readByte();
    }

    /**
     * Write a byte to given output stream.
     *
     * @param output non-null output stream
     * @param value  byte
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeInt8(OutputStream output, byte value) throws IOException {
        output.write(value);
    }

    /**
     * Write a byte to given output stream.
     *
     * @param output non-null output stream
     * @param value  byte
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeInt8(OutputStream output, int value) throws IOException {
        output.write(ClickHouseChecker.between(value, ClickHouseValues.TYPE_INT, Byte.MIN_VALUE, Byte.MAX_VALUE));
    }

    /**
     * Read an unsigned byte as short from given input stream.
     *
     * @param input non-null input stream
     * @return unsigned byte
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static short readUnsignedInt8(ClickHouseInputStream input) throws IOException {
        return (short) (input.readByte() & 0xFF);
    }

    /**
     * Write an unsigned byte to given output stream.
     *
     * @param output non-null output stream
     * @param value  unsigned byte
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeUnsignedInt8(OutputStream output, int value) throws IOException {
        output.write((byte) (0xFF & ClickHouseChecker.between(value, ClickHouseValues.TYPE_INT, 0, U_INT8_MAX)));
    }

    /**
     * Read a short value from given input stream.
     * 
     * @param input non-null input stream
     * @return short value
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static short readInt16(ClickHouseInputStream input) throws IOException {
        return input.readBuffer(2).asShort();
    }

    /**
     * Write a short value to given output stream.
     *
     * @param output non-null output stream
     * @param value  short value
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeInt16(OutputStream output, short value) throws IOException {
        output.write(new byte[] { (byte) (0xFF & value), (byte) (0xFF & (value >> 8)) });
    }

    /**
     * Write a short value to given output stream.
     *
     * @param output non-null output stream
     * @param value  short value
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeInt16(OutputStream output, int value) throws IOException {
        writeInt16(output,
                (short) ClickHouseChecker.between(value, ClickHouseValues.TYPE_INT, Short.MIN_VALUE, Short.MAX_VALUE));
    }

    /**
     * Read an unsigned short value from given input stream.
     *
     * @param input non-null input stream
     * @return unsigned short value
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static int readUnsignedInt16(ClickHouseInputStream input) throws IOException {
        return input.readBuffer(2).asUnsignedShort();
    }

    /**
     * Write an unsigned short value to given output stream.
     *
     * @param output non-null output stream
     * @param value  unsigned short value
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeUnsignedInt16(OutputStream output, int value) throws IOException {
        writeInt16(output,
                (short) (ClickHouseChecker.between(value, ClickHouseValues.TYPE_INT, 0, U_INT16_MAX) & 0xFFFFL));
    }

    /**
     * Read an integer from given input stream.
     *
     * @param input non-null input stream
     * @return integer
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static int readInt32(ClickHouseInputStream input) throws IOException {
        return input.readBuffer(4).asInteger();
    }

    /**
     * Write an integer to given output stream.
     *
     * @param output non-null output stream
     * @param value  integer
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeInt32(OutputStream output, int value) throws IOException {
        output.write(new byte[] { (byte) (0xFF & value), (byte) (0xFF & (value >> 8)), (byte) (0xFF & (value >> 16)),
                (byte) (0xFF & (value >> 24)) });
    }

    /**
     * Read an unsigned integer from given input stream.
     *
     * @param input non-null input stream
     * @return unsigned integer
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static long readUnsignedInt32(ClickHouseInputStream input) throws IOException {
        return input.readBuffer(4).asUnsignedInteger();
    }

    /**
     * Write an unsigned integer to given output stream.
     *
     * @param output non-null output stream
     * @param value  unsigned integer
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeUnsignedInt32(OutputStream output, long value) throws IOException {
        writeInt32(output,
                (int) (ClickHouseChecker.between(value, ClickHouseValues.TYPE_LONG, 0, U_INT32_MAX) & 0xFFFFFFFFL));
    }

    /**
     * Read a long value from given input stream.
     *
     * @param input non-null input stream
     * @return long value
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static long readInt64(ClickHouseInputStream input) throws IOException {
        return input.readBuffer(8).asLong();
    }

    /**
     * Write a long value to given output stream.
     *
     * @param output non-null output stream
     * @param value  long value
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeInt64(OutputStream output, long value) throws IOException {
        byte[] bytes = new byte[8];
        setInt64(bytes, 0, value);
        output.write(bytes);
    }

    /**
     * Read an unsigned long value from given input stream.
     *
     * @param input non-null input stream
     * @return unsigned long value
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static BigInteger readUnsignedInt64(ClickHouseInputStream input) throws IOException {
        return input.readBuffer(8).asUnsignedLong();
    }

    /**
     * Write an unsigned long value to given output stream.
     *
     * @param output non-null output stream
     * @param value  unsigned long value, negative number will be treated as
     *               positive
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeUnsignedInt64(OutputStream output, long value) throws IOException {
        writeInt64(output, value);
    }

    /**
     * Write an unsigned long value to given output stream. Due to overhead of
     * {@link java.math.BigInteger}, this method in general uses more memory and
     * slower than {@link #writeUnsignedInt64(OutputStream, long)}.
     *
     * @param output non-null output stream
     * @param value  unsigned long value
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeUnsignedInt64(OutputStream output, BigInteger value) throws IOException {
        writeInt64(output, ClickHouseChecker
                .between(value, ClickHouseValues.TYPE_BIG_INTEGER, BigInteger.ZERO, U_INT64_MAX).longValue());
    }

    /**
     * Read a big integer(16 bytes) from given input stream.
     *
     * @param input non-null input stream
     * @return big integer
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static BigInteger readInt128(ClickHouseInputStream input) throws IOException {
        return input.readBuffer(16).asBigInteger();
    }

    /**
     * Write a big integer(16 bytes) to given output stream.
     *
     * @param output non-null output stream
     * @param value  big integer
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeInt128(OutputStream output, BigInteger value) throws IOException {
        writeBigInteger(output, value, 16);
    }

    /**
     * Read an unsigned big integer from given input stream.
     *
     * @param input non-null input stream
     * @return unsigned big integer
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static BigInteger readUnsignedInt128(ClickHouseInputStream input) throws IOException {
        return input.readBuffer(16).asUnsignedBigInteger();
    }

    /**
     * Write an unsigned big integer(16 bytes) to given output stream.
     *
     * @param output non-null output stream
     * @param value  unsigned big integer
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeUnsignedInt128(OutputStream output, BigInteger value) throws IOException {
        writeInt128(output,
                ClickHouseChecker.between(value, ClickHouseValues.TYPE_BIG_INTEGER, BigInteger.ZERO, U_INT128_MAX));
    }

    /**
     * Read a big integer(32 bytes) from given input stream.
     *
     * @param input non-null input stream
     * @return big integer
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static BigInteger readInt256(ClickHouseInputStream input) throws IOException {
        return input.readBuffer(32).asBigInteger();
    }

    /**
     * Write a big integer(32 bytes) to given output stream.
     *
     * @param output non-null output stream
     * @param value  big integer
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeInt256(OutputStream output, BigInteger value) throws IOException {
        writeBigInteger(output, value, 32);
    }

    /**
     * Read an unsigned big integer(32 bytes) from given input stream.
     *
     * @param input non-null input stream
     * @return big integer
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static BigInteger readUnsignedInt256(ClickHouseInputStream input) throws IOException {
        return input.readBuffer(32).asUnsignedBigInteger();
    }

    /**
     * Write an unsigned big integer(32 bytes) to given output stream.
     *
     * @param output non-null output stream
     * @param value  unsigned big integer
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeUnsignedInt256(OutputStream output, BigInteger value) throws IOException {
        writeInt256(output,
                ClickHouseChecker.between(value, ClickHouseValues.TYPE_BIG_INTEGER, BigInteger.ZERO, U_INT256_MAX));
    }

    /**
     * Read a float value from given input stream.
     *
     * @param input non-null input stream
     * @return float value
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static float readFloat32(ClickHouseInputStream input) throws IOException {
        return Float.intBitsToFloat(readInt32(input));
    }

    /**
     * Write a float value to given output stream.
     *
     * @param output non-null output stream
     * @param value  float value
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeFloat32(OutputStream output, float value) throws IOException {
        writeInt32(output, Float.floatToIntBits(value));
    }

    /**
     * Read a double value from given input stream.
     *
     * @param input non-null input stream
     * @return double value
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static double readFloat64(ClickHouseInputStream input) throws IOException {
        return Double.longBitsToDouble(readInt64(input));
    }

    /**
     * Write a double value to given output stream.
     *
     * @param output non-null output stream
     * @param value  double value
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeFloat64(OutputStream output, double value) throws IOException {
        writeInt64(output, Double.doubleToLongBits(value));
    }

    /**
     * Read UUID from given input stream.
     *
     * @param input non-null input stream
     * @return UUID
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static UUID readUuid(ClickHouseInputStream input) throws IOException {
        return input.readBuffer(16).asUuid();
    }

    /**
     * Write a UUID to given output stream.
     *
     * @param output non-null output stream
     * @param value  UUID
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeUuid(OutputStream output, java.util.UUID value) throws IOException {
        writeInt64(output, value.getMostSignificantBits());
        writeInt64(output, value.getLeastSignificantBits());
    }

    /**
     * Write a {@code length}-byte long big integer to given output stream.
     *
     * @param output non-null output stream
     * @param value  big integer
     * @param length byte length of the value
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeBigInteger(OutputStream output, BigInteger value, int length) throws IOException {
        byte empty = value.signum() == -1 ? (byte) 0xFF : 0x00;
        byte[] bytes = value.toByteArray();
        int endIndex = bytes.length == length + 1 && bytes[0] == (byte) 0 ? 1 : 0;
        if (bytes.length - endIndex > length) {
            throw new IllegalArgumentException(
                    ClickHouseUtils.format("Expected %d bytes but got %d from: %s", length, bytes.length, value));
        }

        for (int i = bytes.length - 1; i >= endIndex; i--) {
            output.write(bytes[i]);
        }

        for (int i = length - bytes.length; i > 0; i--) {
            output.write(empty);
        }
    }

    /**
     * Read big decimal(4 - 32 bytes) from given input stream.
     *
     * @param input     non-null input stream
     * @param precision precision of the decimal
     * @param scale     scale of the decimal
     * @return big decimal
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static BigDecimal readDecimal(ClickHouseInputStream input, int precision, int scale) throws IOException {
        BigDecimal v;

        if (precision <= ClickHouseDataType.Decimal32.getMaxScale()) {
            v = readDecimal32(input, scale);
        } else if (precision <= ClickHouseDataType.Decimal64.getMaxScale()) {
            v = readDecimal64(input, scale);
        } else if (precision <= ClickHouseDataType.Decimal128.getMaxScale()) {
            v = readDecimal128(input, scale);
        } else {
            v = readDecimal256(input, scale);
        }

        return v;
    }

    /**
     * Write a big decimal(4 - 32 bytes) to given output stream.
     *
     * @param output    non-null output stream
     * @param value     big decimal
     * @param precision precision of the decimal
     * @param scale     scale of the decimal, might be different from
     *                  {@link java.math.BigDecimal#scale()}
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeDecimal(OutputStream output, BigDecimal value, int precision, int scale)
            throws IOException {
        if (precision > ClickHouseDataType.Decimal128.getMaxScale()) {
            writeDecimal256(output, value, scale);
        } else if (precision > ClickHouseDataType.Decimal64.getMaxScale()) {
            writeDecimal128(output, value, scale);
        } else if (precision > ClickHouseDataType.Decimal32.getMaxScale()) {
            writeDecimal64(output, value, scale);
        } else {
            writeDecimal32(output, value, scale);
        }
    }

    /**
     * Read big decimal(4 bytes) from given input stream.
     *
     * @param input non-null input stream
     * @param scale scale of the decimal
     * @return big decimal
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static BigDecimal readDecimal32(ClickHouseInputStream input, int scale) throws IOException {
        return BigDecimal.valueOf(readInt32(input), ClickHouseChecker.between(scale, ClickHouseValues.PARAM_SCALE, 0,
                ClickHouseDataType.Decimal32.getMaxScale()));
    }

    /**
     * Write a big decimal(4 bytes) to given output stream.
     *
     * @param output non-null output stream
     * @param value  big decimal
     * @param scale  scale of the decimal, might be different from
     *               {@link java.math.BigDecimal#scale()}
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeDecimal32(OutputStream output, BigDecimal value, int scale) throws IOException {
        writeInt32(output,
                ClickHouseChecker.between(
                        value.multiply(BigDecimal.TEN.pow(ClickHouseChecker.between(scale, ClickHouseValues.PARAM_SCALE,
                                0, ClickHouseDataType.Decimal32.getMaxScale()))),
                        ClickHouseValues.TYPE_BIG_DECIMAL, DECIMAL32_MIN, DECIMAL32_MAX).intValue());
    }

    /**
     * Read big decimal(8 bytes) from gicen input stream.
     *
     * @param input non-null input stream
     * @param scale scale of the decimal
     * @return big decimal
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static BigDecimal readDecimal64(ClickHouseInputStream input, int scale) throws IOException {
        return BigDecimal.valueOf(readInt64(input), ClickHouseChecker.between(scale, ClickHouseValues.PARAM_SCALE, 0,
                ClickHouseDataType.Decimal64.getMaxScale()));
    }

    /**
     * Write a big decimal(8 bytes) to given output stream.
     *
     * @param output non-null output stream
     * @param value  big decimal
     * @param scale  scale of the decimal, might be different from
     *               {@link java.math.BigDecimal#scale()}
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeDecimal64(OutputStream output, BigDecimal value, int scale) throws IOException {
        writeInt64(output,
                ClickHouseChecker.between(
                        ClickHouseChecker.between(scale, ClickHouseValues.PARAM_SCALE, 0,
                                ClickHouseDataType.Decimal64.getMaxScale()) == 0 ? value
                                        : value.multiply(BigDecimal.TEN.pow(scale)),
                        ClickHouseValues.TYPE_BIG_DECIMAL, DECIMAL64_MIN, DECIMAL64_MAX).longValue());
    }

    /**
     * Read big decimal(16 bytes) from given input stream.
     *
     * @param input non-null input stream
     * @param scale scale of the decimal
     * @return big decimal
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static BigDecimal readDecimal128(ClickHouseInputStream input, int scale) throws IOException {
        return new BigDecimal(readInt128(input), ClickHouseChecker.between(scale, ClickHouseValues.PARAM_SCALE, 0,
                ClickHouseDataType.Decimal128.getMaxScale()));
    }

    /**
     * Write a big decimal(16 bytes) to given output stream.
     *
     * @param output non-null output stream
     * @param value  big decimal
     * @param scale  scale of the decimal, might be different from
     *               {@link java.math.BigDecimal#scale()}
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeDecimal128(OutputStream output, BigDecimal value, int scale) throws IOException {
        writeInt128(output,
                ClickHouseChecker.between(
                        ClickHouseChecker.between(scale, ClickHouseValues.PARAM_SCALE, 0,
                                ClickHouseDataType.Decimal128.getMaxScale()) == 0 ? value
                                        : value.multiply(BigDecimal.TEN.pow(scale)),
                        ClickHouseValues.TYPE_BIG_DECIMAL, DECIMAL128_MIN, DECIMAL128_MAX).toBigInteger());
    }

    /**
     * Read big decimal from given input stream.
     *
     * @param input non-null input stream
     * @param scale scale of the decimal
     * @return big decimal
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static BigDecimal readDecimal256(ClickHouseInputStream input, int scale) throws IOException {
        return new BigDecimal(readInt256(input), ClickHouseChecker.between(scale, ClickHouseValues.PARAM_SCALE, 0,
                ClickHouseDataType.Decimal256.getMaxScale()));
    }

    /**
     * Write a big decimal(32 bytes) to given output stream.
     *
     * @param output non-null output stream
     * @param value  big decimal
     * @param scale  scale of the decimal, might be different from
     *               {@link java.math.BigDecimal#scale()}
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeDecimal256(OutputStream output, BigDecimal value, int scale) throws IOException {
        writeInt256(output,
                ClickHouseChecker.between(
                        ClickHouseChecker.between(scale, ClickHouseValues.PARAM_SCALE, 0,
                                ClickHouseDataType.Decimal256.getMaxScale()) == 0 ? value
                                        : value.multiply(BigDecimal.TEN.pow(scale)),
                        ClickHouseValues.TYPE_BIG_DECIMAL, DECIMAL256_MIN, DECIMAL256_MAX).toBigInteger());
    }

    /**
     * Read {@link java.time.LocalDate} from given input stream.
     *
     * @param input non-null input stream
     * @param tz    time zone for date, could be null
     * @return local date
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static LocalDate readDate(ClickHouseInputStream input, TimeZone tz)
            throws IOException {
        LocalDate d = readDate(input);
        if (tz != null && !tz.toZoneId().equals(ClickHouseValues.SYS_ZONE)) {
            d = d.atStartOfDay(ClickHouseValues.SYS_ZONE).withZoneSameInstant(tz.toZoneId()).toLocalDate();
        }
        return d;
    }

    /**
     * Read {@link java.time.LocalDate} from given input stream.
     *
     * @param input non-null input stream
     * @return local date
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static LocalDate readDate(ClickHouseInputStream input)
            throws IOException {
        return LocalDate.ofEpochDay(readUnsignedInt16(input));
    }

    /**
     * Write a {@link java.time.LocalDate} to given output stream.
     *
     * @param output non-null output stream
     * @param value  local date
     * @param tz     time zone for date, could be null
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeDate(OutputStream output, LocalDate value, TimeZone tz)
            throws IOException {
        if (tz != null && !tz.toZoneId().equals(ClickHouseValues.SYS_ZONE)) {
            value = value.atStartOfDay(tz.toZoneId()).withZoneSameInstant(ClickHouseValues.SYS_ZONE).toLocalDate();
        }
        writeDate(output, value);
    }

    /**
     * Write a {@link java.time.LocalDate} to given output stream.
     *
     * @param output non-null output stream
     * @param value  local date
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeDate(OutputStream output, LocalDate value)
            throws IOException {
        int days = (int) value.toEpochDay();
        writeUnsignedInt16(output, ClickHouseChecker.between(days, ClickHouseValues.TYPE_DATE, 0, U_INT16_MAX));
    }

    /**
     * Read {@link java.time.LocalDate} from given input stream.
     *
     * @param input non-null input stream
     * @param tz    time zone for date, could be null
     * @return local date
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static LocalDate readDate32(ClickHouseInputStream input, TimeZone tz)
            throws IOException {
        LocalDate d = readDate32(input);
        if (tz != null && !tz.toZoneId().equals(ClickHouseValues.SYS_ZONE)) {
            d = d.atStartOfDay(ClickHouseValues.SYS_ZONE).withZoneSameInstant(tz.toZoneId()).toLocalDate();
        }
        return d;
    }

    /**
     * Read {@link java.time.LocalDate} from given input stream.
     *
     * @param input non-null input stream
     * @return local date
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static LocalDate readDate32(ClickHouseInputStream input)
            throws IOException {
        return LocalDate.ofEpochDay(readInt32(input));
    }

    /**
     * Write a {@link java.time.LocalDate} to given output stream.
     *
     * @param output non-null output stream
     * @param value  local date
     * @param tz     time zone for date, could be null
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeDate32(OutputStream output, LocalDate value, TimeZone tz)
            throws IOException {
        if (tz != null && !tz.toZoneId().equals(ClickHouseValues.SYS_ZONE)) {
            value = value.atStartOfDay(tz.toZoneId()).withZoneSameInstant(ClickHouseValues.SYS_ZONE).toLocalDate();
        }
        writeDate32(output, value);
    }

    /**
     * Write a {@link java.time.LocalDate} to given output stream.
     *
     * @param output non-null output stream
     * @param value  local date
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeDate32(OutputStream output, LocalDate value)
            throws IOException {
        writeInt32(output, ClickHouseChecker.between((int) value.toEpochDay(), ClickHouseValues.TYPE_DATE, DATE32_MIN,
                DATE32_MAX));
    }

    /**
     * Read {@link java.time.LocalDateTime} from given input stream.
     *
     * @param input non-null input stream
     * @param tz    time zone, null is treated as UTC
     * @return local datetime
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static LocalDateTime readDateTime(ClickHouseInputStream input, TimeZone tz) throws IOException {
        return readDateTime(input, 0, tz);
    }

    /**
     * Read {@link java.time.LocalDateTime} from given input stream.
     *
     * @param input non-null input stream
     * @param scale scale of the datetime, must between 0 and 9 inclusive
     * @param tz    time zone, null is treated as UTC
     * @return local datetime
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static LocalDateTime readDateTime(ClickHouseInputStream input, int scale, TimeZone tz) throws IOException {
        return ClickHouseChecker.between(scale, ClickHouseValues.PARAM_SCALE, 0,
                ClickHouseDataType.DateTime64.getMaxScale()) == 0 ? readDateTime32(input, tz)
                        : readDateTime64(input, scale, tz);
    }

    /**
     * Write a {@link java.time.LocalDateTime} to given output stream.
     *
     * @param output non-null output stream
     * @param value  local datetime
     * @param tz     time zone, null is treated as UTC
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeDateTime(OutputStream output, LocalDateTime value, TimeZone tz) throws IOException {
        writeDateTime(output, value, 0, tz);
    }

    /**
     * Write a {@link java.time.LocalDateTime} to given output stream.
     *
     * @param output non-null output stream
     * @param value  local datetime
     * @param scale  scale of the datetime, must between 0 and 9 inclusive
     * @param tz     time zone, null is treated as UTC
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeDateTime(OutputStream output, LocalDateTime value, int scale, TimeZone tz)
            throws IOException {
        if (ClickHouseChecker.between(scale, ClickHouseValues.PARAM_SCALE, 0,
                ClickHouseDataType.DateTime64.getMaxScale()) == 0) {
            writeDateTime32(output, value, tz);
        } else {
            writeDateTime64(output, value, scale, tz);
        }
    }

    /**
     * Read {@link java.time.LocalDateTime} from given input stream.
     *
     * @param input non-null input stream
     * @param tz    time zone, null is treated as UTC
     * @return local datetime
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static LocalDateTime readDateTime32(ClickHouseInputStream input, TimeZone tz) throws IOException {
        long time = readUnsignedInt32(input);

        return LocalDateTime.ofInstant(Instant.ofEpochSecond(time < 0L ? 0L : time),
                tz != null ? tz.toZoneId() : ClickHouseValues.UTC_ZONE);
    }

    /**
     * Write a {@link java.time.LocalDateTime} to given output stream.
     *
     * @param output non-null output stream
     * @param value  local datetime
     * @param tz     time zone, null is treated as UTC
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeDateTime32(OutputStream output, LocalDateTime value, TimeZone tz) throws IOException {
        long time = tz == null || tz.equals(ClickHouseValues.UTC_TIMEZONE) ? value.toEpochSecond(ZoneOffset.UTC)
                : value.atZone(tz.toZoneId()).toEpochSecond();

        writeUnsignedInt32(output, ClickHouseChecker.between(time, ClickHouseValues.TYPE_DATE_TIME, 0L, DATETIME_MAX));
    }

    /**
     * Read {@link java.time.LocalDateTime} from given input stream. Same as
     * {@code readDateTime64(input, 3)}.
     * 
     * @param input non-null input stream
     * @param tz    time zone, null is treated as UTC
     * @return local datetime
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static LocalDateTime readDateTime64(ClickHouseInputStream input, TimeZone tz) throws IOException {
        return readDateTime64(input, 3, tz);
    }

    /**
     * Read {@link java.time.LocalDateTime} from given input stream.
     *
     * @param input non-null input stream
     * @param scale scale of the datetime
     * @param tz    time zone, null is treated as UTC
     * @return local datetime
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static LocalDateTime readDateTime64(ClickHouseInputStream input, int scale, TimeZone tz) throws IOException {
        long value = readInt64(input);
        int nanoSeconds = 0;
        if (ClickHouseChecker.between(scale, ClickHouseValues.PARAM_SCALE, 0, 9) > 0) {
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
                tz != null ? tz.toZoneId() : ClickHouseValues.UTC_ZONE);
    }

    /**
     * Write a {@link java.time.LocalDateTime} to given output stream. Same as
     * {@code writeDateTime64(output, value, 3)}.
     *
     * @param output non-null output stream
     * @param value  local datetime
     * @param tz     time zone, null is treated as UTC
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeDateTime64(OutputStream output, LocalDateTime value, TimeZone tz) throws IOException {
        writeDateTime64(output, value, 3, tz);
    }

    /**
     * Write a {@link java.time.LocalDateTime} to given output stream.
     *
     * @param output non-null output stream
     * @param value  local datetime
     * @param scale  scale of the datetime, must between 0 and 9 inclusive
     * @param tz     time zone, null is treated as UTC
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeDateTime64(OutputStream output, LocalDateTime value, int scale, TimeZone tz)
            throws IOException {
        long v = ClickHouseChecker.between(
                tz == null || tz.equals(ClickHouseValues.UTC_TIMEZONE) ? value.toEpochSecond(ZoneOffset.UTC)
                        : value.atZone(tz.toZoneId()).toEpochSecond(),
                ClickHouseValues.TYPE_DATE_TIME,
                DATETIME64_MIN,
                scale == 9 ? DATETIME64_9_MAX : DATETIME64_MAX);
        if (ClickHouseChecker.between(scale, ClickHouseValues.PARAM_SCALE, 0, 9) > 0) {
            v *= BASES[scale];
            int nanoSeconds = value.getNano();
            if (nanoSeconds > 0L) {
                v += nanoSeconds / BASES[9 - scale];
            }
        }

        writeInt64(output, v);
    }

    /**
     * Read string with fixed length from given input stream.
     *
     * @param input  non-null input stream
     * @param length byte length of the string
     * @return string with fixed length
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static String readFixedString(ClickHouseInputStream input, int length) throws IOException {
        return readFixedString(input, length, null);
    }

    /**
     * Read string with fixed length from given input stream.
     *
     * @param input   non-null input stream
     * @param length  byte length of the string
     * @param charset charset used to convert string to byte array, null means UTF-8
     * @return string with fixed length
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static String readFixedString(ClickHouseInputStream input, int length, Charset charset) throws IOException {
        return input.readBuffer(length).asString(charset);
    }

    /**
     * Write a string with fixed length to given output stream.
     *
     * @param output non-null output stream
     * @param value  string
     * @param length byte length of the string
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeFixedString(OutputStream output, String value, int length) throws IOException {
        writeFixedString(output, value, length, null);
    }

    /**
     * Write a string with fixed length to given output stream.
     *
     * @param output  non-null output stream
     * @param value   string
     * @param length  byte length of the string
     * @param charset charset used to convert string to byte array, null means UTF-8
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeFixedString(OutputStream output, String value, int length, Charset charset)
            throws IOException {
        byte[] src = ClickHouseChecker.notLongerThan(value.getBytes(charset == null ? StandardCharsets.UTF_8 : charset),
                "value", length);

        byte[] bytes = new byte[length];
        System.arraycopy(src, 0, bytes, 0, src.length);

        output.write(bytes);
    }

    /**
     * Reads characters from given reader.
     *
     * @param input  non-null reader
     * @param length length in character
     * @return string value
     * @throws IOException when failed to read value from reader or reached end of
     *                     the stream
     */
    public static String readString(Reader input, int length) throws IOException {
        return new String(readCharacters(input, length));
    }

    /**
     * Write a string to given output stream.
     *
     * @param output non-null output stream
     * @param value  string
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeString(OutputStream output, String value) throws IOException {
        writeString(output, value, null);
    }

    /**
     * Write a string to given output stream.
     *
     * @param output  non-null output stream
     * @param value   string
     * @param charset charset used to convert string to byte array, null means UTF-8
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeString(OutputStream output, String value, Charset charset) throws IOException {
        byte[] bytes = value.getBytes(charset == null ? StandardCharsets.UTF_8 : charset);

        writeVarInt(output, bytes.length);
        output.write(bytes);
    }

    /**
     * Writes a binary string to given output stream.
     *
     * @param output non-null output stream
     * @param value  non-null byte array
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeString(OutputStream output, byte[] value) throws IOException {
        writeVarInt(output, value.length);
        output.write(value);
    }

    /**
     * Read varint from given input stream.
     *
     * @param input non-null input stream
     * @return varint
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static int readVarInt(InputStream input) throws IOException {
        // https://github.com/ClickHouse/ClickHouse/blob/abe314feecd1647d7c2b952a25da7abf5c19f352/src/IO/VarInt.h#L126
        long result = 0L;
        int shift = 0;
        for (int i = 0; i < 9; i++) {
            // gets 7 bits from next byte
            int b = input.read();
            if (b == -1) {
                try {
                    input.close();
                } catch (IOException e) {
                    // ignore error
                }
                throw new EOFException();
            }
            result |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                break;
            }
            shift += 7;
        }

        return (int) result;
    }

    /**
     * Read varint from given byte buffer.
     *
     * @param buffer non-null byte buffer
     * @return varint
     */
    public static int readVarInt(ByteBuffer buffer) {
        long result = 0L;
        int shift = 0;
        for (int i = 0; i < 9; i++) {
            // gets 7 bits from next byte
            byte b = buffer.get();
            result |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                break;
            }
            shift += 7;
        }

        return (int) result;
    }

    /**
     * Write varint to given output stream.
     *
     * @param output non-null output stream
     * @param value  long value
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeVarInt(OutputStream output, long value) throws IOException {
        // https://github.com/ClickHouse/ClickHouse/blob/abe314feecd1647d7c2b952a25da7abf5c19f352/src/IO/VarInt.h#L187
        for (int i = 0; i < 9; i++) {
            byte b = (byte) (value & 0x7F);

            if (value > 0x7F) {
                b |= 0x80;
            }

            value >>= 7;
            output.write(b);

            if (value == 0) {
                return;
            }
        }
    }

    /**
     * Write varint to given output stream.
     *
     * @param buffer non-null byte buffer
     * @param value  integer value
     */
    public static void writeVarInt(ByteBuffer buffer, int value) {
        while ((value & 0xFFFFFF80) != 0) {
            buffer.put((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        buffer.put((byte) (value & 0x7F));
    }

    private BinaryStreamUtils() {
    }
}

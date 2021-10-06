package com.clickhouse.client.data;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;
import com.clickhouse.client.ClickHouseChecker;
import com.clickhouse.client.ClickHouseUtils;
import com.clickhouse.client.ClickHouseValues;

/**
 * Utility class for dealing with binary stream and data.
 */
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

    public static final int DATE32_MAX = (int) LocalDate.of(2283, 11, 11).toEpochDay();
    public static final int DATE32_MIN = (int) LocalDate.of(1925, 1, 1).toEpochDay();

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

    public static final long DATETIME64_MAX = LocalDateTime.of(LocalDate.of(2283, 11, 11), LocalTime.MAX)
            .toEpochSecond(ZoneOffset.UTC);
    public static final long DATETIME64_MIN = LocalDateTime.of(LocalDate.of(1925, 1, 1), LocalTime.MIN)
            .toEpochSecond(ZoneOffset.UTC);

    public static final long MILLIS_IN_DAY = TimeUnit.DAYS.toMillis(1);

    public static final long DATETIME_MAX = U_INT32_MAX * 1000L;

    public static final BigDecimal NANOS = new BigDecimal(BigInteger.TEN.pow(9));

    private static <T extends Enum<T>> T toEnum(int value, Class<T> enumType) {
        for (T t : ClickHouseChecker.nonNull(enumType, "enumType").getEnumConstants()) {
            if (t.ordinal() == value) {
                return t;
            }
        }

        throw new IllegalArgumentException(
                ClickHouseUtils.format("Enum [%s] does not contain value [%d]", enumType, value));
    }

    /**
     * Reverse the given byte array.
     * 
     * @param bytes byte array to manipulate
     * @return same byte array but reserved
     */
    public static byte[] reverse(byte[] bytes) {
        if (bytes != null && bytes.length > 1) {
            for (int i = 0, len = bytes.length / 2; i < len; i++) {
                byte b = bytes[i];
                bytes[i] = bytes[bytes.length - 1 - i];
                bytes[bytes.length - 1 - i] = b;
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
     * Read an unsigned byte from given input stream.
     *
     * @param input non-null input stream
     * @return unnsigned byte which is always greater than or equal to zero
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static int readUnsignedByte(InputStream input) throws IOException {
        int value = input.read();
        if (value == -1) {
            try {
                input.close();
            } catch (IOException e) {
                // ignore
            }

            throw new EOFException();
        }

        return value;
    }

    /**
     * Read a byte from given input stream.
     *
     * @param input non-null input stream
     * @return byte
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static byte readByte(InputStream input) throws IOException {
        return (byte) readUnsignedByte(input);
    }

    /**
     * Read {@code size} bytes from given input stream. It behaves in the same way
     * as {@link java.io.DataInput#readFully(byte[])}.
     *
     * @param input input stream
     * @param size  number of bytes to read
     * @return byte array and its length should be {@code size}
     * @throws IOException when failed to read value from input stream, not able to
     *                     retrieve all bytes, or reached end of the stream
     */
    public static byte[] readBytes(InputStream input, int size) throws IOException {
        int count = 0;
        byte[] bytes = new byte[size];
        while (count < size) {
            int n = input.read(bytes, count, size - count);
            if (n < 0) {
                try {
                    input.close();
                } catch (IOException e) {
                    // ignore
                }

                throw count == 0 ? new EOFException()
                        : new IOException(ClickHouseUtils
                                .format("Reached end of input stream after reading %d of %d bytes", count, size));
            }
            count += n;
        }

        return bytes;
    }

    /**
     * Read boolean from given input stream. It uses {@link #readByte(InputStream)}
     * to get value and return {@code true} only when the value is {@code 1}.
     *
     * @param input non-null input stream
     * @return boolean
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static boolean readBoolean(InputStream input) throws IOException {
        return ClickHouseChecker.between(readByte(input), ClickHouseValues.TYPE_BOOLEAN, 0, 1) == 1;
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
    public static <T extends Enum<T>> T readEnum8(InputStream input, Class<T> enumType) throws IOException {
        return toEnum(readEnum8(input), enumType);
    }

    /**
     * Read enum value from given input stream. Same as
     * {@link #readInt8(InputStream)}.
     *
     * @param input non-null input stream
     * @return enum value
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static byte readEnum8(InputStream input) throws IOException {
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
    public static <T extends Enum<T>> T readEnum16(InputStream input, Class<T> enumType) throws IOException {
        return toEnum(readEnum16(input), enumType);
    }

    /**
     * Read enum value from given input stream. Same as
     * {@link #readInt16(InputStream)}.
     *
     * @param input non-null input stream
     * @return enum value
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static short readEnum16(InputStream input) throws IOException {
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
    public static double[] readGeoPoint(InputStream input) throws IOException {
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
    public static double[][] readGeoRing(InputStream input) throws IOException {
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
    public static double[][][] readGeoPolygon(InputStream input) throws IOException {
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
    public static double[][][][] readGeoMultiPolygon(InputStream input) throws IOException {
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
     * {@link #readBoolean(InputStream)}.
     * 
     * @param input non-null input stream
     * @return true if it's null; false otherwise
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static boolean readNull(InputStream input) throws IOException {
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
    public static Inet4Address readInet4Address(InputStream input) throws IOException {
        return (Inet4Address) InetAddress.getByAddress(reverse(readBytes(input, 4)));
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
    public static Inet6Address readInet6Address(InputStream input) throws IOException {
        return Inet6Address.getByAddress(null, readBytes(input, 16), null);
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
     * Read a byte from given input stream. Same as {@link #readByte(InputStream)}.
     *
     * @param input non-null input stream
     * @return byte
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static byte readInt8(InputStream input) throws IOException {
        return readByte(input);
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
    public static short readUnsignedInt8(InputStream input) throws IOException {
        return (short) (readByte(input) & 0xFFL);
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
        output.write((byte) (ClickHouseChecker.between(value, ClickHouseValues.TYPE_INT, 0, U_INT8_MAX) & 0xFFL));
    }

    /**
     * Read a short value from given input stream.
     * 
     * @param input non-null input stream
     * @return short value
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static short readInt16(InputStream input) throws IOException {
        byte[] bytes = readBytes(input, 2);
        return (short) ((0xFF & bytes[0]) | (bytes[1] << 8));
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
        output.write(new byte[] { (byte) (0xFFL & value), (byte) (0xFFL & (value >> 8)) });
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
    public static int readUnsignedInt16(InputStream input) throws IOException {
        return (int) (readInt16(input) & 0xFFFFL);
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
    public static int readInt32(InputStream input) throws IOException {
        byte[] bytes = readBytes(input, 4);

        return (0xFF & bytes[0]) | ((0xFF & bytes[1]) << 8) | ((0xFF & bytes[2]) << 16) | (bytes[3] << 24);
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
        output.write(new byte[] { (byte) (0xFFL & value), (byte) (0xFFL & (value >> 8)), (byte) (0xFFL & (value >> 16)),
                (byte) (0xFFL & (value >> 24)) });
    }

    /**
     * Read an unsigned integer from given input stream.
     *
     * @param input non-null input stream
     * @return unsigned integer
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static long readUnsignedInt32(InputStream input) throws IOException {
        return readInt32(input) & 0xFFFFFFFFL;
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
    public static long readInt64(InputStream input) throws IOException {
        byte[] bytes = readBytes(input, 8);

        return (0xFFL & bytes[0]) | ((0xFFL & bytes[1]) << 8) | ((0xFFL & bytes[2]) << 16) | ((0xFFL & bytes[3]) << 24)
                | ((0xFFL & bytes[4]) << 32) | ((0xFFL & bytes[5]) << 40) | ((0xFFL & bytes[6]) << 48)
                | ((0xFFL & bytes[7]) << 56);
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
        value = Long.reverseBytes(value);

        byte[] bytes = new byte[8];
        for (int i = 7; i >= 0; i--) {
            bytes[i] = (byte) (value & 0xFFL);
            value >>= 8;
        }

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
    public static BigInteger readUnsignedInt64(InputStream input) throws IOException {
        return new BigInteger(1, reverse(readBytes(input, 8)));
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
    public static BigInteger readInt128(InputStream input) throws IOException {
        return new BigInteger(reverse(readBytes(input, 16)));
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
    public static BigInteger readUnsignedInt128(InputStream input) throws IOException {
        return new BigInteger(1, reverse(readBytes(input, 16)));
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
    public static BigInteger readInt256(InputStream input) throws IOException {
        return new BigInteger(reverse(readBytes(input, 32)));
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
    public static BigInteger readUnsignedInt256(InputStream input) throws IOException {
        return new BigInteger(1, reverse(readBytes(input, 32)));
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
    public static float readFloat32(InputStream input) throws IOException {
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
    public static double readFloat64(InputStream input) throws IOException {
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
    public static java.util.UUID readUuid(InputStream input) throws IOException {
        return new java.util.UUID(readInt64(input), readInt64(input));
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
    public static BigDecimal readDecimal(InputStream input, int precision, int scale) throws IOException {
        BigDecimal v;

        if (precision <= 9) {
            v = readDecimal32(input, scale);
        } else if (precision <= 18) {
            v = readDecimal64(input, scale);
        } else if (precision <= 38) {
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
        if (precision > 38) {
            writeDecimal256(output, value, scale);
        } else if (precision > 18) {
            writeDecimal128(output, value, scale);
        } else if (precision > 9) {
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
    public static BigDecimal readDecimal32(InputStream input, int scale) throws IOException {
        return BigDecimal.valueOf(readInt32(input),
                ClickHouseChecker.between(scale, ClickHouseValues.PARAM_SCALE, 0, 9));
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
                        value.multiply(BigDecimal.TEN
                                .pow(ClickHouseChecker.between(scale, ClickHouseValues.PARAM_SCALE, 0, 9))),
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
    public static BigDecimal readDecimal64(InputStream input, int scale) throws IOException {
        return BigDecimal.valueOf(readInt64(input),
                ClickHouseChecker.between(scale, ClickHouseValues.PARAM_SCALE, 0, 18));
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
                        ClickHouseChecker.between(scale, ClickHouseValues.PARAM_SCALE, 0, 18) == 0 ? value
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
    public static BigDecimal readDecimal128(InputStream input, int scale) throws IOException {
        return new BigDecimal(readInt128(input), ClickHouseChecker.between(scale, ClickHouseValues.PARAM_SCALE, 0, 38));
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
                        ClickHouseChecker.between(scale, ClickHouseValues.PARAM_SCALE, 0, 38) == 0 ? value
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
    public static BigDecimal readDecimal256(InputStream input, int scale) throws IOException {
        return new BigDecimal(readInt256(input), ClickHouseChecker.between(scale, ClickHouseValues.PARAM_SCALE, 0, 76));
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
                        ClickHouseChecker.between(scale, ClickHouseValues.PARAM_SCALE, 0, 76) == 0 ? value
                                : value.multiply(BigDecimal.TEN.pow(scale)),
                        ClickHouseValues.TYPE_BIG_DECIMAL, DECIMAL256_MIN, DECIMAL256_MAX).toBigInteger());
    }

    /**
     * Read {@link java.time.LocalDate} from given input stream.
     *
     * @param input non-null input stream
     * @return local date
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static LocalDate readDate(InputStream input) throws IOException {
        return LocalDate.ofEpochDay(readUnsignedInt16(input));
    }

    /**
     * Write a {@link java.time.LocalDate} to given output stream.
     *
     * @param output non-null output stream
     * @param value  local date
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeDate(OutputStream output, LocalDate value) throws IOException {
        int days = (int) value.toEpochDay();
        writeUnsignedInt16(output, ClickHouseChecker.between(days, ClickHouseValues.TYPE_DATE, 0, U_INT16_MAX));
    }

    /**
     * Read {@link java.time.LocalDate} from given input stream.
     *
     * @param input non-null input stream
     * @return local date
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static LocalDate readDate32(InputStream input) throws IOException {
        return LocalDate.ofEpochDay(readInt32(input));
    }

    /**
     * Write a {@link java.time.LocalDate} to given output stream.
     *
     * @param output non-null output stream
     * @param value  local date
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeDate32(OutputStream output, LocalDate value) throws IOException {
        writeInt32(output, ClickHouseChecker.between((int) value.toEpochDay(), ClickHouseValues.TYPE_DATE, DATE32_MIN,
                DATE32_MAX));
    }

    /**
     * Read {@link java.time.LocalDateTime} from given input stream.
     *
     * @param input non-null input stream
     * @return local datetime
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static LocalDateTime readDateTime(InputStream input) throws IOException {
        return readDateTime(input, 0);
    }

    /**
     * Read {@link java.time.LocalDateTime} from given input stream.
     *
     * @param input non-null input stream
     * @param scale scale of the datetime, must between 0 and 9 inclusive
     * @return local datetime
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static LocalDateTime readDateTime(InputStream input, int scale) throws IOException {
        return ClickHouseChecker.between(scale, ClickHouseValues.PARAM_SCALE, 0, 9) == 0 ? readDateTime32(input)
                : readDateTime64(input, scale);
    }

    /**
     * Write a {@link java.time.LocalDateTime} to given output stream.
     *
     * @param output non-null output stream
     * @param value  local datetime
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeDateTime(OutputStream output, LocalDateTime value) throws IOException {
        writeDateTime(output, value, 0);
    }

    /**
     * Write a {@link java.time.LocalDateTime} to given output stream.
     *
     * @param output non-null output stream
     * @param value  local datetime
     * @param scale  scale of the datetime, must between 0 and 9 inclusive
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeDateTime(OutputStream output, LocalDateTime value, int scale) throws IOException {
        if (ClickHouseChecker.between(scale, ClickHouseValues.PARAM_SCALE, 0, 9) == 0) {
            writeDateTime32(output, value);
        } else {
            writeDateTime64(output, value, scale);
        }
    }

    /**
     * Read {@link java.time.LocalDateTime} from given input stream.
     *
     * @param input non-null input stream
     * @return local datetime
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static LocalDateTime readDateTime32(InputStream input) throws IOException {
        long time = readUnsignedInt32(input);

        return LocalDateTime.ofEpochSecond(time < 0L ? 0L : time, 0, ZoneOffset.UTC);
    }

    /**
     * Write a {@link java.time.LocalDateTime} to given output stream.
     *
     * @param output non-null output stream
     * @param value  local datetime
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeDateTime32(OutputStream output, LocalDateTime value) throws IOException {
        long time = value.toEpochSecond(ZoneOffset.UTC);

        writeUnsignedInt32(output, ClickHouseChecker.between(time, ClickHouseValues.TYPE_DATE_TIME, 0L, DATETIME_MAX));
    }

    /**
     * Read {@link java.time.LocalDateTime} from given input stream. Same as
     * {@code readDateTime64(input, 3)}.
     * 
     * @param input non-null input stream
     * @return local datetime
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static LocalDateTime readDateTime64(InputStream input) throws IOException {
        return readDateTime64(input, 3);
    }

    /**
     * Read {@link java.time.LocalDateTime} from given input stream.
     *
     * @param input non-null input stream
     * @param scale scale of the datetime
     * @return local datetime
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static LocalDateTime readDateTime64(InputStream input, int scale) throws IOException {
        long value = readInt64(input);
        int nanoSeconds = 0;
        if (ClickHouseChecker.between(scale, ClickHouseValues.PARAM_SCALE, 0, 9) > 0) {
            int factor = 1;
            for (int i = 0; i < scale; i++) {
                factor *= 10;
            }

            nanoSeconds = (int) (value % factor);
            value /= factor;
            if (nanoSeconds < 0) {
                nanoSeconds += factor;
                value--;
            }
            if (nanoSeconds > 0L) {
                for (int i = 9 - scale; i > 0; i--) {
                    nanoSeconds *= 10;
                }
            }
        }

        return LocalDateTime.ofEpochSecond(value, nanoSeconds, ZoneOffset.UTC);
    }

    /**
     * Write a {@link java.time.LocalDateTime} to given output stream. Same as
     * {@code writeDateTime64(output, value, 3)}.
     *
     * @param output non-null output stream
     * @param value  local datetime
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeDateTime64(OutputStream output, LocalDateTime value) throws IOException {
        writeDateTime64(output, value, 3);
    }

    /**
     * Write a {@link java.time.LocalDateTime} to given output stream.
     *
     * @param output non-null output stream
     * @param value  local datetime
     * @param scale  scale of the datetime, must between 0 and 9 inclusive
     * @throws IOException when failed to write value to output stream or reached
     *                     end of the stream
     */
    public static void writeDateTime64(OutputStream output, LocalDateTime value, int scale) throws IOException {
        long v = ClickHouseChecker.between(value.toEpochSecond(ZoneOffset.UTC), ClickHouseValues.TYPE_DATE_TIME,
                DATETIME64_MIN, DATETIME64_MAX);
        if (ClickHouseChecker.between(scale, ClickHouseValues.PARAM_SCALE, 0, 9) > 0) {
            for (int i = 0; i < scale; i++) {
                v *= 10;
            }
            int nanoSeconds = value.getNano();
            if (nanoSeconds > 0L) {
                for (int i = 9 - scale; i > 0; i--) {
                    nanoSeconds /= 10;
                }
                v += nanoSeconds;
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
    public static String readFixedString(InputStream input, int length) throws IOException {
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
    public static String readFixedString(InputStream input, int length, Charset charset) throws IOException {
        byte[] bytes = readBytes(input, length);

        return new String(bytes, charset == null ? StandardCharsets.UTF_8 : charset);
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
     * Read string from given input stream.
     *
     * @param input non-null input stream
     * @return string value
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static String readString(InputStream input) throws IOException {
        return readString(input, null);
    }

    /**
     * Read string from given input stream.
     *
     * @param input   non-null input stream
     * @param charset charset used to convert byte array to string, null means UTF-8
     * @return string value
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public static String readString(InputStream input, Charset charset) throws IOException {
        return new String(readBytes(input, readVarInt(input)), charset == null ? StandardCharsets.UTF_8 : charset);
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
            int b = readUnsignedByte(input);
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
            int b = buffer.get();
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

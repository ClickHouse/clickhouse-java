package com.clickhouse.client;

import java.io.Serializable;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * This class represents a slice of a byte array. It holds a reference to a byte
 * array, and it uses {@link #position()} and {@link #length()} to define the
 * slice. You may think of it as a lite version of {@link java.nio.ByteBuffer}.
 */
public class ClickHouseByteBuffer implements Serializable {
    private static final long serialVersionUID = -8178041799873465082L;

    /**
     * Empty byte array.
     */
    public static final byte[] EMPTY_BYTES = new byte[0];

    /**
     * Empty and read-only byte buffer.
     */
    public static final ByteBuffer EMPTY_BUFFER = ByteBuffer.wrap(EMPTY_BYTES).asReadOnlyBuffer();

    /**
     * Creates an empty byte buffer.
     *
     * @return empty byte buffer
     */
    public static ClickHouseByteBuffer newInstance() {
        return new ClickHouseByteBuffer(EMPTY_BYTES, 0, 0);
    }

    /**
     * Wraps given byte array as byte buffer.
     *
     * @param bytes byte array
     * @return non-null byte buffer
     */
    public static ClickHouseByteBuffer of(byte[] bytes) {
        return bytes == null || bytes.length == 0 ? newInstance() : new ClickHouseByteBuffer(bytes, 0, bytes.length);
    }

    /**
     * Wraps given byte array as byte buffer.
     *
     * @param bytes  byte array
     * @param offset start position
     * @param length length
     * @return non-null byte buffer
     */
    public static ClickHouseByteBuffer of(byte[] bytes, int offset, int length) {
        if (bytes == null || bytes.length == 0 || length == 0) {
            return newInstance();
        } else {
            validate(bytes, offset, length);
        }

        return new ClickHouseByteBuffer(bytes, offset, length);
    }

    /**
     * Wraps given byte arrays as byte buffer.
     *
     * @param list   list of non-null byte arrays
     * @param offset offset
     * @param length length
     * @return non-null byte buffer
     */
    public static ClickHouseByteBuffer of(List<byte[]> list, int offset, int length) {
        ClickHouseByteBuffer buf = newInstance();
        if (list == null || list.isEmpty() || length == 0) {
            return buf;
        }

        return buf.update(list, offset, length);
    }

    static void validate(byte[] bytes, int offset, int length) {
        int len = ClickHouseChecker.nonNull(bytes, "Byte array").length;
        if (ClickHouseChecker.between(offset, "Offset", 0, len)
                + ClickHouseChecker.between(length, "Length", 0, len) > len) {
            throw new IllegalArgumentException(
                    ClickHouseUtils.format("Offset(%d) plus length(%d) should not greater than %d", offset, length,
                            len));
        }
    }

    protected byte[] array;
    protected int position;
    protected int length;

    /**
     * Default constructor.
     *
     * @param bytes  non-null byte array
     * @param offset start position
     * @param length length of bytes
     */
    protected ClickHouseByteBuffer(byte[] bytes, int offset, int length) {
        this.array = bytes;
        this.position = offset;
        this.length = length;
    }

    /**
     * Converts 4 bytes(little-endian) to Int32. Same as {@code getInteger(0)}.
     *
     * @return Int32 value
     * @deprecated will be removed in v0.3.3, please use {@link #asInteger()}
     *             instead
     */
    @Deprecated
    public int asInt32() {
        return getInteger(0);
    }

    /**
     * Converts 4 bytes(little-endian) to UInt32. Same as
     * {@code getUnsignedInteger(0)}.
     *
     * @return UInt32 value
     * @deprecated will be removed in v0.3.3, please use
     *             {@link #asUnsignedInteger()} instead
     */
    @Deprecated
    public long asUnsignedInt32() {
        return getUnsignedInteger(0);
    }

    /**
     * Converts 4 bytes(little-endian) starting from {@code position() + offset} to
     * Int32.
     *
     * @param offset zero-based relative offset, 1 means the second byte starting
     *               from {@link #position()}
     * @return Int32 value
     * @deprecated will be removed in v0.3.3, please use {@link #getInteger(int)}
     *             instead
     */
    @Deprecated
    public int getInt32(int offset) {
        return getInteger(offset);
    }

    /**
     * Converts 4 bytes(little-endian) starting from {@code position() + offset} to
     * UInt32.
     *
     * @param offset zero-based relative offset, 1 means the second byte starting
     *               from {@link #position()}
     * @return UInt32 value
     * @deprecated will be removed in v0.3.3, please use
     *             {@link #getUnsignedInteger(int)} instead
     */
    @Deprecated
    public long getUnsignedInt32(int offset) {
        return getUnsignedInteger(offset);
    }

    /**
     * Converts 8 bytes(little-endian) to Int64. Same as {@code getLong(0)}.
     *
     * @return Int64 value
     * @deprecated will be removed in v0.3.3, please use {@link #asLong()} instead
     */
    @Deprecated
    public long asInt64() {
        return asLong();
    }

    /**
     * Converts 8 bytes(little-endian) to unsigned Int64. Same as
     * {@code getBigInteger(0, 8, true)}.
     *
     * @return unsigned Int64 value
     * @deprecated will be removed in v0.3.3, please use {@link #asUnsignedLong()}
     *             instead
     */
    @Deprecated
    public BigInteger asUnsignedInt64() {
        return getBigInteger(0, 8, true);
    }

    /**
     * Converts 8 bytes(little-endian) starting from {@code position() + offset} to
     * Int64.
     *
     * @param offset zero-based relative offset, 1 means the second byte starting
     *               from {@link #position()}
     * @return Int64 value
     * @deprecated will be removed in v0.3.3, please use {@link #getLong(int)}
     *             instead
     */
    @Deprecated
    public long getInt64(int offset) {
        return getLong(offset);
    }

    /**
     * Converts all bytes(little-endian) to signed big integer. Same as
     * {@code getBigInteger(0, length(), false)}.
     *
     * @return non-null signed big integer
     */
    public BigInteger asBigInteger() {
        return getBigInteger(0, length, false);
    }

    public double asDouble() {
        return getDouble(0);
    }

    public double[] asDoubleArray() {
        int step = 8;
        int len = length / step;
        double[] values = new double[len];
        for (int i = 0, offset = 0; i < len; i++, offset += step) {
            values[i] = getDouble(offset);
        }
        return values;
    }

    public float asFloat() {
        return getFloat(0);
    }

    public float[] asFloatArray() {
        int step = 4;
        int len = length / step;
        float[] values = new float[len];
        for (int i = 0, offset = 0; i < len; i++, offset += step) {
            values[i] = getFloat(offset);
        }
        return values;
    }

    /**
     * Converts all bytes(little-endian) to unsigned big integer. Same as
     * {@code getBigInteger(0, length(), true)}.
     *
     * @return non-null unsigned big integer
     */
    public BigInteger asUnsignedBigInteger() {
        return getBigInteger(0, length, true);
    }

    /**
     * Converts {@code byteLength} bytes(little-endian) starting from
     * {@code position() + offset} to big integer.
     *
     * @param offset     zero-based relative offset, 1 means the second byte
     *                   starting from {@link #position()}
     * @param byteLength bytes to convert
     * @param unsigned   true if it's unsigned big integer; false otherwise
     * @return non-null big integer
     */
    public BigInteger getBigInteger(int offset, int byteLength, boolean unsigned) {
        byte[] bytes = new byte[byteLength];
        int startIndex = offset + position;
        for (int i = startIndex + byteLength - 1, j = 0; i >= startIndex; i--, j++) {
            bytes[j] = array[i];
        }
        return unsigned ? new BigInteger(1, bytes) : new BigInteger(bytes);
    }

    /**
     * Converts 8 bytes(little-endian) starting from {@code position() + offset} to
     * double.
     *
     * @param offset zero-based relative offset, 1 means the second byte
     *               starting from {@link #position()}
     * @return double
     */
    public double getDouble(int offset) {
        return Double.longBitsToDouble(getLong(offset));
    }

    /**
     * Converts 4 bytes(little-endian) starting from {@code position() + offset} to
     * float.
     *
     * @param offset zero-based relative offset, 1 means the second byte
     *               starting from {@link #position()}
     * @return float
     */
    public float getFloat(int offset) {
        return Float.intBitsToFloat(getInteger(offset));
    }

    /**
     * Converts 4 bytes(little-endian) starting from {@code position() + offset} to
     * signed integer.
     *
     * @param offset zero-based relative offset, 1 means the second byte
     *               starting from {@link #position()}
     * @return signed integer
     */
    public int getInteger(int offset) {
        int i = offset + position;
        return (0xFF & array[i]) | ((0xFF & array[i + 1]) << 8) | ((0xFF & array[i + 2]) << 16)
                | ((0xFF & array[i + 3]) << 24);
    }

    /**
     * Converts 8 bytes(little-endian) starting from {@code position() + offset} to
     * signed long.
     *
     * @param offset zero-based relative offset, 1 means the second byte
     *               starting from {@link #position()}
     * @return signed long
     */
    public long getLong(int offset) {
        int i = offset + position;
        return (0xFFL & array[i]) | ((0xFFL & array[i + 1]) << 8) | ((0xFFL & array[i + 2]) << 16)
                | ((0xFFL & array[i + 3]) << 24) | ((0xFFL & array[i + 4]) << 32)
                | ((0xFFL & array[i + 5]) << 40) | ((0xFFL & array[i + 6]) << 48)
                | ((0xFFL & array[i + 7]) << 56);
    }

    /**
     * Converts 2 bytes(little-endian) starting from {@code position() + offset} to
     * signed short.
     *
     * @param offset zero-based relative offset, 1 means the second byte
     *               starting from {@link #position()}
     * @return signed short
     */
    public short getShort(int offset) {
        int i = offset + position;
        return (short) ((0xFF & array[i]) | (array[i + 1] << 8));
    }

    /**
     * Converts 4 bytes(little-endian) starting from {@code position() + offset} to
     * unsigned integer.
     *
     * @param offset zero-based relative offset, 1 means the second byte
     *               starting from {@link #position()}
     * @return unsigned integer
     */
    public long getUnsignedInteger(int offset) {
        return 0xFFFFFFFFL & getInteger(offset);
    }

    /**
     * Converts 8 bytes(little-endian) starting from {@code position() + offset} to
     * unsigned long.
     *
     * @param offset zero-based relative offset, 1 means the second byte
     *               starting from {@link #position()}
     * @return non-null unsigned long
     */
    public BigInteger getUnsignedLong(int offset) {
        return getBigInteger(0, 8, true);
    }

    /**
     * Converts 2 bytes(little-endian) starting from {@code position() + offset} to
     * unsigned short.
     *
     * @param offset zero-based relative offset, 1 means the second byte
     *               starting from {@link #position()}
     * @return unsigned short
     */
    public int getUnsignedShort(int offset) {
        return 0xFFFF & getShort(offset);
    }

    public BigInteger[] asBigIntegerArray(int byteLength, boolean unsigned) {
        int len = length / byteLength;
        BigInteger[] values = new BigInteger[len];
        for (int i = 0, offset = 0; i < len; i++, offset += byteLength) {
            values[i] = getBigInteger(offset, byteLength, unsigned);
        }
        return values;
    }

    public int asInteger() {
        return getInteger(0);
    }

    public long asLong() {
        return getLong(0);
    }

    public short asShort() {
        return getShort(0);
    }

    public int[] asIntegerArray() {
        int step = 4;
        int len = length / step;
        int[] values = new int[len];
        for (int i = 0, offset = 0; i < len; i++, offset += step) {
            values[i] = getInteger(offset);
        }
        return values;
    }

    public long[] asLongArray() {
        int step = 8;
        int len = length / step;
        long[] values = new long[len];
        for (int i = 0, offset = 0; i < len; i++, offset += step) {
            values[i] = getLong(offset);
        }
        return values;
    }

    public short[] asShortArray() {
        int step = 2;
        int len = length / step;
        short[] values = new short[len];
        for (int i = 0, offset = 0; i < len; i++, offset += step) {
            values[i] = getShort(offset);
        }
        return values;
    }

    public long asUnsignedInteger() {
        return getUnsignedInteger(0);
    }

    public BigInteger asUnsignedLong() {
        return getUnsignedLong(0);
    }

    public int asUnsignedShort() {
        return getUnsignedShort(0);
    }

    public long[] asUnsignedIntegerArray() {
        int step = 4;
        int len = length / step;
        long[] values = new long[len];
        for (int i = 0, offset = 0; i < len; i++, offset += step) {
            values[i] = getUnsignedInteger(offset);
        }
        return values;
    }

    public int[] asUnsignedShortArray() {
        int step = 2;
        int len = length / step;
        int[] values = new int[len];
        for (int i = 0, offset = 0; i < len; i++, offset += step) {
            values[i] = getUnsignedShort(offset);
        }
        return values;
    }

    public UUID asUuid() {
        return getUuid(0);
    }

    public UUID getUuid(int offset) {
        return new UUID(getLong(offset), getLong(offset + 8));
    }

    /**
     * Converts to ASCII string.
     * 
     * @return non-null ASCII string
     */
    public String asAsciiString() {
        return asString(StandardCharsets.US_ASCII);
    }

    /**
     * Converts to string using given charset.
     *
     * @param charset optional charset, null is treated as
     *                {@link StandardCharsets#UTF_8}
     * @return non-null string
     */
    public String asString(Charset charset) {
        if (length < 1) {
            return "";
        }
        return new String(array, position, length, charset != null ? charset : StandardCharsets.UTF_8);
    }

    /**
     * Converts to UTF-8 string.
     *
     * @return non-null UTF-8 string
     */
    public String asUnicodeString() {
        return asString(StandardCharsets.UTF_8);
    }

    /**
     * Compacts byte array by creating a new copy with exact same {@code length} but
     * {@code position} changed to zero. It does nothing when {@code position} is
     * zero and {@code length} is same as {@code bytes.length}.
     *
     * @return this byte buffer
     */
    public ClickHouseByteBuffer compact() {
        if (position > 0 || length < array.length) {
            byte[] bytes = new byte[length];
            System.arraycopy(array, position, bytes, 0, length);
            this.array = bytes;
            this.position = 0;
        }

        return this;
    }

    /**
     * Checks whether the buffer is compact or not. A buffer is compact when
     * {@code position} points to zero and {@code length} equals to
     * {@code array().length}.
     *
     * @return true if the buffer is compact; false otherwise
     */
    public boolean isCompact() {
        return position == 0 && array.length == length;
    }

    /**
     * Checks whether the buffer is empty or not. Please pay attention that this
     * will return {@code true} when {@link #length()} is zero, even
     * {@link #array()} may return an non-empty byte array.
     *
     * @return true if the buffer is empty; false otherwise
     */
    public boolean isEmpty() {
        return length < 1;
    }

    /**
     * Resets the buffer to empty.
     *
     * @return this byte buffer
     */
    public ClickHouseByteBuffer reset() {
        array = EMPTY_BYTES;
        position = 0;
        length = 0;
        return this;
    }

    /**
     * Reverses the byte array.
     *
     * @return this byte buffer
     */
    public ClickHouseByteBuffer reverse() {
        return reverse(0, length);
    }

    /**
     * Reverses the byte array.
     *
     * @param offset start position
     * @param length bytes to reserve
     * @return this byte buffer
     */
    public ClickHouseByteBuffer reverse(int offset, int length) {
        if (length > 1) {
            offset += position;
            int l = offset + length;
            for (int i = offset, len = i + length / 2; i < len; i++) {
                byte b = array[i];
                --l;
                array[i] = array[l];
                array[l] = b;
            }
        }
        return this;
    }

    /**
     * Gets slice of the byte buffer.
     *
     * @param offset offset zero-based relative offset, 1 means the second byte
     *               starting from {@link #position()}
     * @param length length of the slice
     * @return non-null slice of the byte buffer(backed by the same byte array but
     *         with different position and length)
     */
    public ClickHouseByteBuffer slice(int offset, int length) {
        if (offset == 0 && length == this.length) {
            return this;
        }

        return ClickHouseByteBuffer.of(array, offset + position, length);
    }

    /**
     * Updates buffer.
     *
     * @param bytes byte array, null is same as empty byte array
     * @return this byte buffer
     */
    public ClickHouseByteBuffer update(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            reset();
        } else {
            position = 0;
            length = (array = bytes).length;
        }

        return this;
    }

    /**
     * Updates buffer.
     *
     * @param bytes  byte array, null is same as empty byte array
     * @param offset start position
     * @param length length of bytes
     * @return this byte buffer
     */
    public ClickHouseByteBuffer update(byte[] bytes, int offset, int length) {
        if (bytes == null || bytes.length == 0 || length == 0) {
            return reset();
        } else {
            validate(bytes, offset, length);
        }

        if (bytes != this.array) {
            this.array = bytes;
        }
        this.position = offset;
        this.length = length;
        return this;
    }

    /**
     * Updates buffer.
     *
     * @param list   list of byte arrays, null is same as empty byte array
     * @param offset start position
     * @param length length of bytes
     * @return this byte buffer
     */
    public ClickHouseByteBuffer update(List<byte[]> list, int offset, int length) {
        if (list == null || list.isEmpty() || length == 0) {
            return reset();
        } else if (list.size() == 1) {
            return update(list.get(0), offset, length);
        }

        byte[][] bb = new byte[list.size()][];
        int limit = offset + length;
        int len = 0;
        int index = 0;
        int startIndex = -1;
        int startOffset = -1;
        int endIndex = -1;
        int endOffset = -1;
        for (byte[] bytes : list) {
            len += ClickHouseChecker.nonNull(bytes, "Byte array").length;
            if (startIndex < 0 && len > offset) {
                startIndex = index;
                startOffset = offset - (len - bytes.length);
            }
            if (endIndex < 0 && len >= limit) {
                endIndex = index;
                endOffset = limit - (len - bytes.length);
            }
            bb[index++] = bytes;
        }
        if (ClickHouseChecker.between(offset, "Offset", 0, len)
                + ClickHouseChecker.between(length, "Length", 0, len) > len) {
            throw new IllegalArgumentException(
                    ClickHouseUtils.format("Offset(%d) plus length(%d) should not greater than %d", offset, length,
                            len));
        }

        if (startIndex == endIndex) {
            this.array = bb[startIndex];
            this.position = startOffset;
            this.length = length;
        } else {
            this.array = new byte[length];
            byte[] bytes = bb[startIndex];
            System.arraycopy(bytes, startOffset, this.array, 0, bytes.length - startOffset);
            startOffset = bytes.length - startOffset;
            for (int i = startIndex + 1; i < endIndex; i++) {
                bytes = bb[i];
                System.arraycopy(bytes, 0, this.array, startOffset, bytes.length);
                startOffset += bytes.length;
            }
            bytes = bb[endIndex];
            System.arraycopy(bytes, 0, this.array, startOffset, endOffset);
            this.position = 0;
            this.length = length;
        }
        return this;
    }

    /**
     * Gets byte array.
     *
     * @return non-null byte array
     */
    public byte[] array() {
        return array;
    }

    public byte firstByte() {
        return array[position];
    }

    public byte lastByte() {
        return array[position + length - 1];
    }

    /**
     * Gets start position.
     *
     * @return start position
     */
    public int position() {
        return position;
    }

    /**
     * Sets new position.
     *
     * @param newPosition new position, which should be always less than
     *                    {@code limit()}
     * @return this byte buffer
     */
    public ClickHouseByteBuffer position(int newPosition) {
        this.position = newPosition;
        return this;
    }

    /**
     * Gets length of bytes.
     *
     * @return length of bytes
     */
    public int length() {
        return length;
    }

    /**
     * Gets end position.
     *
     * @return end position
     */
    public int limit() {
        return position + length;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = prime + Arrays.hashCode(array);
        result = prime * result + position;
        result = prime * result + length;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ClickHouseByteBuffer other = (ClickHouseByteBuffer) obj;
        return Arrays.equals(array, other.array) && position == other.position && length == other.length;
    }

    @Override
    public String toString() {
        return new StringBuilder().append(getClass().getSimpleName()).append("array=").append(array)
                .append(", position=").append(position).append(", length=").append(length).append(')').toString();
    }
}

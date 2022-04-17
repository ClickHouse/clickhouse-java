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

    // public byte[] toInt8Array()

    /**
     * Converts 4 bytes to Int32. Same as {@code getInt32(0)}.
     *
     * @return Int32 value
     */
    public int asInt32() {
        return getInt32(0);
    }

    public long asUnsignedInt32() {
        return getUnsignedInt32(0);
    }

    /**
     * Converts 4 bytes starting from {@code position() + offset} to Int32.
     *
     * @param offset zero-based relative offset, 1 means the second byte starting
     *               from {@link #position()}
     * @return Int32 value
     */
    public int getInt32(int offset) {
        offset += position;
        return (0xFF & array[offset++]) | ((0xFF & array[offset++]) << 8) | ((0xFF & array[offset++]) << 16)
                | ((0xFF & array[offset]) << 24);
    }

    public long getUnsignedInt32(int offset) {
        return 0xFFFFFFFFL & getInt32(offset);
    }

    /**
     * Converts 8 bytes to Int64. Same as {@code getInt64(0)}.
     *
     * @return Int64 value
     */
    public long asInt64() {
        return getInt64(0);
    }

    public BigInteger asUnsignedInt64() {
        return getBigInteger(0, 8, true);
    }

    /**
     * Converts 8 bytes starting from {@code position() + offset} to Int64.
     *
     * @param offset zero-based relative offset, 1 means the second byte starting
     *               from {@link #position()}
     * @return Int64 value
     */
    public long getInt64(int offset) {
        offset += position;
        return (0xFFL & array[offset++]) | ((0xFFL & array[offset++]) << 8) | ((0xFFL & array[offset++]) << 16)
                | ((0xFFL & array[offset++]) << 24) | ((0xFFL & array[offset++]) << 32)
                | ((0xFFL & array[offset++]) << 40) | ((0xFFL & array[offset++]) << 48)
                | ((0xFFL & array[offset]) << 56);
    }

    public BigInteger asBigInteger() {
        return getBigInteger(0, length, false);
    }

    public BigInteger asUnsignedBigInteger() {
        return getBigInteger(0, length, true);
    }

    public BigInteger getBigInteger(int offset, int byteLength, boolean unsigned) {
        reverse(offset, byteLength);
        // return new BigInteger(1, array, offset + position, byteLength);
        // just for supporting JDK 8 :<
        byte[] bytes = array;
        if (offset != 0 || array.length != byteLength) {
            bytes = new byte[byteLength];
            System.arraycopy(array, offset + position, bytes, 0, byteLength);
        }
        return unsigned ? new BigInteger(1, bytes) : new BigInteger(bytes);
    }

    public UUID asUuid() {
        return getUuid(0);
    }

    public UUID getUuid(int offset) {
        return new UUID(getInt64(offset), getInt64(offset + 8));
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
            array = bytes;
            position = 0;
            length = bytes.length;
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

        this.array = bytes;
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

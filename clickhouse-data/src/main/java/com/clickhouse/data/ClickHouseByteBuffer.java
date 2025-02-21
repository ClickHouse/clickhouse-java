package com.clickhouse.data;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * This class represents a slice of a byte array. It holds a reference to a byte
 * array, and it uses {@link #position()} and {@link #length()} to define the
 * slice. You may think of it as a lite version of {@link java.nio.ByteBuffer}.
 */
@Deprecated
public final class ClickHouseByteBuffer implements Serializable {
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
     * Wraps given byte buffer.
     *
     * @param buffer byte buffer
     * @return non-null wrapped byte buffer
     */
    public static ClickHouseByteBuffer of(ByteBuffer buffer) {
        if (buffer == null || !buffer.hasRemaining()) {
            return newInstance();
        }

        int pos = buffer.position();
        int len = buffer.remaining();
        byte[] bytes;
        if (buffer.hasArray()) {
            bytes = buffer.array();
        } else {
            bytes = new byte[len];
            buffer.get(bytes);
            ((Buffer) buffer).position(pos);
            pos = 0;
        }
        return new ClickHouseByteBuffer(bytes, pos, len);
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
     * Converts all bytes(little-endian) to signed big integer. Same as
     * {@code getBigInteger(0, length(), false)}.
     *
     * @return non-null signed big integer
     */
    public BigInteger asBigInteger() {
        return getBigInteger(0, length, false);
    }

    public BigDecimal asBigDecimal() {
        return asBigDecimal(0);
    }

    public BigDecimal asBigDecimal(int scale) {
        return new BigDecimal(asBigInteger(), scale);
    }

    public boolean asBoolean() {
        return getBoolean(0);
    }

    public boolean[] asBooleanArray() {
        int len = length;
        boolean[] values = new boolean[len];
        for (int i = 0; i < len; i++) {
            values[i] = array[position + i] == (byte) 1;
        }
        return values;
    }

    public double asDouble() {
        return getDouble(0);
    }

    public double[] asDoubleArray() {
        int step = Double.BYTES;
        int len = length / step;
        double[] values = new double[len];
        for (int i = 0, offset = 0; i < len; i++, offset += step) {
            values[i] = getDouble(offset);
        }
        return values;
    }

    public LocalDate asDate() {
        return LocalDate.ofEpochDay(asUnsignedInteger());
    }

    public LocalDateTime asDateTime() {
        return asDateTime(0);
    }

    public LocalDateTime asDateTime(int scale) {
        return ClickHouseValues
                .convertToDateTime(asBigDecimal(ClickHouseChecker.between(scale, ClickHouseValues.PARAM_SCALE, 0, 9)));
    }

    public float asFloat() {
        return getFloat(0);
    }

    public float[] asFloatArray() {
        int step = Float.BYTES;
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
     * Converts the byte at {@code position() + offset} in array to boolean.
     *
     * @param offset zero-based relative offset, 1 means the second byte
     *               starting from {@link #position()}
     * @return boolean
     */
    public boolean getBoolean(int offset) {
        return array[offset + position] == (byte) 1;
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
        return ClickHouseByteUtils.getFloat64(array, offset + position);
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
        return ClickHouseByteUtils.getFloat32(array, offset + position);
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
        return ClickHouseByteUtils.getInt32(array, offset + position);
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
        return ClickHouseByteUtils.getInt64(array, offset + position);
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
        return ClickHouseByteUtils.getInt16(array, offset + position);
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
        int step = Integer.BYTES;
        int len = length / step;
        int[] values = new int[len];
        for (int i = 0, offset = 0; i < len; i++, offset += step) {
            values[i] = getInteger(offset);
        }
        return values;
    }

    public long[] asLongArray() {
        int step = Long.BYTES;
        int len = length / step;
        long[] values = new long[len];
        for (int i = 0, offset = 0; i < len; i++, offset += step) {
            values[i] = getLong(offset);
        }
        return values;
    }

    public short[] asShortArray() {
        int step = Short.BYTES;
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
        int step = Integer.BYTES;
        int len = length / step;
        long[] values = new long[len];
        for (int i = 0, offset = 0; i < len; i++, offset += step) {
            values[i] = getUnsignedInteger(offset);
        }
        return values;
    }

    public int[] asUnsignedShortArray() {
        int step = Short.BYTES;
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
     * Checks if the byte buffer is same as the given byte array.
     *
     * @param bytes bytes to check
     * @return true if the byte buffer is same as the given byte array; false
     *         otherwise
     */
    public boolean match(byte[] bytes) {
        boolean matched = false;
        int len = bytes == null ? 0 : bytes.length;
        if (len == this.length) {
            matched = true;
            for (int i = 0; i < len; i++) {
                if (bytes[i] != array[position + i]) {
                    matched = false;
                    break;
                }
            }
        }
        return matched;
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
     * @param buffer byte buffer
     * @return this byte buffer
     */
    public ClickHouseByteBuffer update(ByteBuffer buffer) {
        if (buffer == null || !buffer.hasRemaining()) {
            return reset();
        }

        int pos = buffer.position();
        int len = buffer.remaining();
        byte[] bytes;
        if (buffer.hasArray()) {
            bytes = buffer.array();
        } else {
            bytes = new byte[len];
            buffer.get(bytes);
            ((Buffer) buffer).position(pos);
            pos = 0;
        }
        return update(bytes, pos, len);
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
//            validate(bytes, offset, length);
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

    /**
     * Creates a copy of the current byte buffer.
     *
     * @param deep true to copy the underlying byte array; false to reuse
     * @return non-null copy of the current byte buffer
     */
    public ClickHouseByteBuffer copy(boolean deep) {
        byte[] bytes;
        int pos;
        if (deep) {
            bytes = new byte[length];
            pos = 0;
            System.arraycopy(array, position, bytes, 0, length);
        } else {
            bytes = array;
            pos = position;
        }
        return ClickHouseByteBuffer.of(bytes, pos, length);
    }

    public byte firstByte() {
        return array[position];
    }

    public byte getByte(int offset) {
        return array[position + ClickHouseChecker.between(offset, "Offset", 0, length)];
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

    /**
     * Sets new length.
     *
     * @param newLength new length, negative number is treated as zero
     * @return this byte buffer
     */
    public ClickHouseByteBuffer setLength(int newLength) {
        this.length = newLength < 0 ? 0 : newLength;
        return this;
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

package com.clickhouse.data;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

@Deprecated
public final class ClickHouseByteUtils {
    // partial class
    private static final ByteUtils BU = new ByteUtils(ByteOrder.LITTLE_ENDIAN);

    private static int indexOfFull(byte[] bytes, int bpos, int blen, byte[] search, int spos, int slen) {
        outer: for (int i = bpos, len = bpos + blen - slen + 1; i < len; i++) { // NOSONAR
            for (int j = 0, l = slen; j < l; j++) {
                if (bytes[i + j] != search[j + spos]) {
                    continue outer;
                }
            }
            return i;
        }
        return -1;
    }

    private static int indexOfPartial(byte[] bytes, int bpos, int blen, byte[] search, int spos, int slen) {
        int matchedPos = -1;
        outer: for (int i = bpos, len = bpos + blen; i < len; i++) { // NOSONAR
            for (int j = 0, l = Math.min(slen, len - i); j < l; j++) {
                if (bytes[i + j] != search[j + spos]) {
                    matchedPos = -1;
                    continue outer;
                } else if (matchedPos == -1) {
                    matchedPos = i;
                }
            }
            return i;
        }
        return matchedPos;
    }

    public static boolean equals(byte[] a, int aFromIndex, int aToIndex, byte[] b, int bFromIndex, int bToIndex) {
        return BU.equals(a, aFromIndex, aToIndex, b, bFromIndex, bToIndex);
    }

    /**
     * Reads a 16-bit integer in little-endian byte order from the given byte array
     * starting at the specified offset.
     *
     * @param bytes  the byte array containing the integer value
     * @param offset the starting position of the integer value within the byte
     *               array
     * @return the 16-bit integer value read from the byte array
     */
    public static short getInt16(byte[] bytes, int offset) {
        return BU.getInt16(bytes, offset);
    }

    public static void setInt16(byte[] bytes, int offset, short value) {
        BU.setInt16(bytes, offset, value);
    }

    /**
     * Reads a 32-bit integer in little-endian byte order from the given byte array
     * starting at the specified offset.
     *
     * @param bytes  the byte array containing the integer value
     * @param offset the starting position of the integer value within the byte
     *               array
     * @return the 32-bit integer value read from the byte array
     */
    public static int getInt32(byte[] bytes, int offset) {
        return BU.getInt32(bytes, offset);
    }

    public static void setInt32(byte[] bytes, int offset, int value) {
        BU.setInt32(bytes, offset, value);
    }

    public static long getInt64(byte[] bytes, int offset) {
        return BU.getInt64(bytes, offset);
    }

    public static void setInt64(byte[] bytes, int offset, long value) {
        BU.setInt64(bytes, offset, value);
    }

    public static float getFloat32(byte[] bytes, int offset) {
        return BU.getFloat32(bytes, offset);
    }

    public static void setFloat32(byte[] bytes, int offset, float value) {
        BU.setFloat32(bytes, offset, value);
    }

    public static double getFloat64(byte[] bytes, int offset) {
        return BU.getFloat64(bytes, offset);
    }

    public static void setFloat64(byte[] bytes, int offset, double value) {
        BU.setFloat64(bytes, offset, value);
    }

    public static byte[] getOrCopy(ByteBuffer buffer, int length) {
        if (buffer == null || length <= 0) {
            return new byte[0];
        }

        // let it fail later when buffer.remaining() < length

        byte[] bytes;
        if (buffer.hasArray() && length == buffer.capacity()) {
            bytes = buffer.array();
        } else {
            int position = buffer.position();
            bytes = new byte[length];
            buffer.get(bytes);
            ((Buffer) buffer).position(position);
        }
        return bytes;
    }

    /**
     * Returns the index of the first occurrence of the specified {@code search}
     * array within the given {@code bytes} array. Same as
     * {@code indexOf(bytes, 0, bytes.length, search, 0, search.length, false)}.
     *
     * @param bytes  the byte array in which to search for the specified
     *               {@code search} array.
     * @param search the byte array to search for within the {@code bytes} array.
     * @return the index of the first occurrence of the search array within the
     *         bytes array, or -1 if the search array is not found
     */
    public static int indexOf(byte[] bytes, byte[] search) {
        if (bytes == null || search == null) {
            return -1;
        }

        return indexOf(bytes, 0, bytes.length, search, 0, search.length, false);
    }

    /**
     * Returns the index of the first occurrence of the specified {@code search}
     * array within the given {@code bytes} array, considering specified positions
     * and lengths for both arrays.
     *
     * @param bytes  the byte array in which to search for the specified
     *               {@code search} array.
     * @param bpos   the starting position in the {@code bytes} array from which to
     *               search
     * @param blen   the number of bytes in the {@code bytes} array to consider for
     *               searching
     * @param search the byte array to search for within the {@code bytes} array.
     * @param spos   the starting position in the {@code search} array to consider
     *               for matching
     * @param slen   the number of bytes in the {@code search} array to consider for
     *               matching
     * @return the index of the first occurrence of the search array within the
     *         bytes array, or -1 if the search array is not found
     */
    public static int indexOf(byte[] bytes, int bpos, int blen, byte[] search, int spos, int slen) {
        return indexOf(bytes, bpos, blen, search, spos, slen, false);
    }

    /**
     * Returns the index of the first occurrence of the specified {@code search}
     * array within the given {@code bytes} array, considering specified positions
     * and lengths for both arrays and partial matching at the end of the bytes
     * array.
     *
     * @param bytes   the byte array in which to search for the specified
     *                {@code search} array.
     * @param bpos    the starting position in the {@code bytes} array from which to
     *                search
     * @param blen    the number of bytes in the {@code bytes} array to consider for
     *                searching
     * @param search  the byte array to search for within the {@code bytes} array.
     * @param spos    the starting position in the {@code search} array to consider
     *                for matching
     * @param slen    the number of bytes in the {@code search} array to consider
     *                for matching
     * @param partial whether partial match should be considered
     * @return the index of the first occurrence of the search array within the
     *         bytes array, or -1 if the search array is not found
     */
    public static int indexOf(byte[] bytes, int bpos, int blen, byte[] search, int spos, int slen, boolean partial) {
        if (bytes == null || search == null || bpos < 0 || blen <= 0 || spos < 0 || slen < 0
                || bytes.length < (bpos + blen) || search.length < (spos + slen) || (!partial && blen < slen)) {
            return -1;
        } else if (slen == 0) {
            return 0;
        }

        return partial ? indexOfPartial(bytes, bpos, blen, search, spos, slen)
                : indexOfFull(bytes, bpos, blen, search, spos, slen);
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
     * Gets varint from given byte buffer.
     *
     * @param buffer non-null byte buffer
     * @return varint
     */
    public static int getVarInt(ByteBuffer buffer) {
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
     * @param buffer non-null byte buffer
     * @param value  integer value
     */
    public static void setVarInt(ByteBuffer buffer, int value) {
        while ((value & 0xFFFFFF80) != 0) {
            buffer.put((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        buffer.put((byte) (value & 0x7F));
    }

    /**
     * Reads varint from given input stream.
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
     * Writes varint to given output stream.
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

    private ClickHouseByteUtils() {
    }
}

package com.clickhouse.data;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class ClickHouseByteUtils {
    public static int getInt32LE(byte[] bytes, int offset) {
        return (0xFF & bytes[offset++]) | ((0xFF & bytes[offset++]) << 8) | ((0xFF & bytes[offset++]) << 16)
                | ((0xFF & bytes[offset]) << 24);
    }

    public static void setInt32LE(byte[] bytes, int offset, int value) {
        bytes[offset++] = (byte) (0xFF & value);
        bytes[offset++] = (byte) (0xFF & (value >> 8));
        bytes[offset++] = (byte) (0xFF & (value >> 16));
        bytes[offset] = (byte) (0xFF & (value >> 24));
    }

    public static long getInt64LE(byte[] bytes, int offset) {
        return (0xFFL & bytes[offset++]) | ((0xFFL & bytes[offset++]) << 8) | ((0xFFL & bytes[offset++]) << 16)
                | ((0xFFL & bytes[offset++]) << 24) | ((0xFFL & bytes[offset++]) << 32)
                | ((0xFFL & bytes[offset++]) << 40) | ((0xFFL & bytes[offset++]) << 48)
                | ((0xFFL & bytes[offset]) << 56);
    }

    public static void setInt64LE(byte[] bytes, int offset, long value) {
        bytes[offset++] = (byte) (0xFF & value);
        bytes[offset++] = (byte) (0xFF & (value >> 8));
        bytes[offset++] = (byte) (0xFF & (value >> 16));
        bytes[offset++] = (byte) (0xFF & (value >> 24));
        bytes[offset++] = (byte) (0xFF & (value >> 32));
        bytes[offset++] = (byte) (0xFF & (value >> 40));
        bytes[offset++] = (byte) (0xFF & (value >> 48));
        bytes[offset] = (byte) (0xFF & (value >> 56));
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

    protected ClickHouseByteUtils() {
    }
}

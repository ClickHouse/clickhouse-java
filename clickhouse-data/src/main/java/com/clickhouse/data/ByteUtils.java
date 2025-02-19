package com.clickhouse.data;

import java.nio.ByteOrder;

@Deprecated
public final class ByteUtils {
    public boolean equals(byte[] a, int aFromIndex, int aToIndex, byte[] b, int bFromIndex, int bToIndex) {
        int aLen = aToIndex - aFromIndex;
        int bLen = bToIndex - bFromIndex;

        boolean matched;
        if (matched = (aLen == bLen)) {
            for (int i = 0; i < aLen; i++) {
                if (a[aFromIndex + i] != b[bFromIndex + i]) {
                    matched = false;
                    break;
                }
            }
        }
        return matched;
    }

    public byte getInt8(byte[] bytes, int offset) {
        return bytes[offset];
    }

    public void setInt8(byte[] bytes, int offset, byte value) {
        bytes[offset] = value;
    }

    public short getInt16(byte[] bytes, int offset) {
        return (short) ((0xFF & bytes[offset]) | (bytes[offset + 1] << 8));
    }

    public void setInt16(byte[] bytes, int offset, short value) {
        bytes[offset] = (byte) (0xFF & value);
        bytes[offset + 1] = (byte) (0xFF & (value >> 8));
    }

    public int getInt32(byte[] bytes, int offset) {
        return (0xFF & bytes[offset]) | ((0xFF & bytes[offset + 1]) << 8) | ((0xFF & bytes[offset + 2]) << 16)
                | ((0xFF & bytes[offset + 3]) << 24);
    }

    public void setInt32(byte[] bytes, int offset, int value) {
        bytes[offset] = (byte) (0xFF & value);
        bytes[offset + 1] = (byte) (0xFF & (value >> 8));
        bytes[offset + 2] = (byte) (0xFF & (value >> 16));
        bytes[offset + 3] = (byte) (0xFF & (value >> 24));
    }

    public long getInt64(byte[] bytes, int offset) {
        return (0xFFL & bytes[offset]) | ((0xFFL & bytes[offset + 1]) << 8) | ((0xFFL & bytes[offset + 2]) << 16)
                | ((0xFFL & bytes[offset + 3]) << 24) | ((0xFFL & bytes[offset + 4]) << 32)
                | ((0xFFL & bytes[offset + 5]) << 40) | ((0xFFL & bytes[offset + 6]) << 48)
                | ((0xFFL & bytes[offset + 7]) << 56);
    }

    public void setInt64(byte[] bytes, int offset, long value) {
        bytes[offset] = (byte) (0xFF & value);
        bytes[offset + 1] = (byte) (0xFF & (value >> 8));
        bytes[offset + 2] = (byte) (0xFF & (value >> 16));
        bytes[offset + 3] = (byte) (0xFF & (value >> 24));
        bytes[offset + 4] = (byte) (0xFF & (value >> 32));
        bytes[offset + 5] = (byte) (0xFF & (value >> 40));
        bytes[offset + 6] = (byte) (0xFF & (value >> 48));
        bytes[offset + 7] = (byte) (0xFF & (value >> 56));
    }

    public float getFloat32(byte[] bytes, int offset) {
        return Float.intBitsToFloat(getInt32(bytes, offset));
    }

    public void setFloat32(byte[] bytes, int offset, float value) {
        setInt32(bytes, offset, Float.floatToIntBits(value));
    }

    public double getFloat64(byte[] bytes, int offset) {
        return Double.longBitsToDouble(getInt64(bytes, offset));
    }

    public void setFloat64(byte[] bytes, int offset, double value) {
        setInt64(bytes, offset, Double.doubleToLongBits(value));
    }

    ByteUtils(ByteOrder byteOrder) {
    }
}

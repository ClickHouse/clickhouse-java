package com.clickhouse.data;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;

public final class ByteUtils {
    public boolean equals(byte[] a, int aFromIndex, int aToIndex, byte[] b, int bFromIndex, int bToIndex) {
        return Arrays.equals(a, aFromIndex, aToIndex, b, bFromIndex, bToIndex);
    }

    public byte getInt8(byte[] bytes, int offset) {
        return (byte) byteHandle.get(bytes, offset);
    }

    public void setInt8(byte[] bytes, int offset, byte value) {
        byteHandle.set(bytes, offset, value);
    }

    public short getInt16(byte[] bytes, int offset) {
        return (short) shortHandle.get(bytes, offset);
    }

    public void setInt16(byte[] bytes, int offset, short value) {
        shortHandle.set(bytes, offset, value);
    }

    public int getInt32(byte[] bytes, int offset) {
        return (int) intHandle.get(bytes, offset);
    }

    public void setInt32(byte[] bytes, int offset, int value) {
        intHandle.set(bytes, offset, value);
    }

    public long getInt64(byte[] bytes, int offset) {
        return (long) longHandle.get(bytes, offset);
    }

    public void setInt64(byte[] bytes, int offset, long value) {
        longHandle.set(bytes, offset, value);
    }

    public float getFloat32(byte[] bytes, int offset) {
        return (float) floatHandle.get(bytes, offset);
    }

    public void setFloat32(byte[] bytes, int offset, float value) {
        floatHandle.set(bytes, offset, value);
    }

    public double getFloat64(byte[] bytes, int offset) {
        return (double) doubleHandle.get(bytes, offset);
    }

    public void setFloat64(byte[] bytes, int offset, double value) {
        doubleHandle.set(bytes, offset, value);
    }

    private final VarHandle byteHandle;
    private final VarHandle shortHandle;
    private final VarHandle intHandle;
    private final VarHandle longHandle;
    private final VarHandle floatHandle;
    private final VarHandle doubleHandle;

    ByteUtils(ByteOrder byteOrder) {
        byteHandle = MethodHandles.arrayElementVarHandle(byte[].class);
        shortHandle = MethodHandles.byteArrayViewVarHandle(short[].class, byteOrder);
        intHandle = MethodHandles.byteArrayViewVarHandle(int[].class, byteOrder);
        longHandle = MethodHandles.byteArrayViewVarHandle(long[].class, byteOrder);
        floatHandle = MethodHandles.byteArrayViewVarHandle(float[].class, byteOrder);
        doubleHandle = MethodHandles.byteArrayViewVarHandle(double[].class, byteOrder);
    }
}

package com.clickhouse.data;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteOrder;

import sun.misc.Unsafe;

@SuppressWarnings("squid:S3011")
public final class UnsafeByteUtils {
    public static final ByteOrder NATIVE_BYTE_ORDER = ByteOrder.nativeOrder();

    private static final Unsafe UNSAFE;

    private static final long BYTE_ARRAY_OFFSET = Unsafe.ARRAY_BYTE_BASE_OFFSET;
    private static final long LONG_ARRAY_OFFSET = Unsafe.ARRAY_LONG_BASE_OFFSET;

    static {
        Unsafe instance;
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            instance = (Unsafe) f.get(null);
        } catch (NoSuchFieldException e) {
            try {
                Constructor<Unsafe> c = Unsafe.class.getDeclaredConstructor();
                c.setAccessible(true);
                instance = c.newInstance();
            } catch (RuntimeException | NoSuchMethodException | InstantiationException | IllegalAccessException
                    | InvocationTargetException ex) {
                throw new ExceptionInInitializerError(ex);
            }
        } catch (Exception e) {
            throw new ExceptionInInitializerError("Cannot access Unsafe");
        }
        UNSAFE = instance;
    }

    public static byte getByte(byte[] bytes, int offset) {
        return UNSAFE.getByte(bytes, BYTE_ARRAY_OFFSET + offset);
    }

    public static int getInteger(byte[] bytes, int offset) {
        int v = UNSAFE.getInt(bytes, BYTE_ARRAY_OFFSET + offset);
        if (NATIVE_BYTE_ORDER == ByteOrder.BIG_ENDIAN) {
            v = Integer.reverseBytes(v);
        }
        return v;
    }

    public static long getLong(byte[] bytes, int offset) {
        if (NATIVE_BYTE_ORDER == ByteOrder.BIG_ENDIAN) {
            return (0xFFL & getByte(bytes, offset)) | ((0xFFL & getByte(bytes, offset +
                    1)) << 8)
                    | ((0xFFL & getByte(bytes, offset + 2)) << 16)
                    | ((0xFFL & getByte(bytes, offset + 3) << 24) | ((0xFFL & getByte(bytes,
                            offset + 4)) << 32)
                            | ((0xFFL & getByte(bytes, offset + 5)) << 40) | ((0xFFL & getByte(bytes,
                                    offset + 6)) << 48)
                            | ((0xFFL & getByte(bytes, offset + 7)) << 56));
        }
        return UNSAFE.getLong(bytes, BYTE_ARRAY_OFFSET + offset);
    }

    public static void putLong(long[] values, int offset, long value) {
        UNSAFE.putLong(values, Unsafe.ARRAY_LONG_BASE_OFFSET + Unsafe.ARRAY_LONG_INDEX_SCALE * offset, value);
    }

    public static short getShort(byte[] bytes, int offset) {
        short v = UNSAFE.getShort(bytes, BYTE_ARRAY_OFFSET + offset);
        if (NATIVE_BYTE_ORDER == ByteOrder.BIG_ENDIAN) {
            v = Short.reverseBytes(v);
        }
        return v;
    }

    public static void setByte(byte[] bytes, int offset, byte value) {
        UNSAFE.putByte(bytes, BYTE_ARRAY_OFFSET + Unsafe.ARRAY_BYTE_INDEX_SCALE * offset, value);
    }

    public static void setInteger(byte[] bytes, int offset, int value) {
        setByte(bytes, offset, (byte) value);
        setByte(bytes, offset + 1, (byte) (value >>> 8));
        setByte(bytes, offset + 2, (byte) (value >>> 16));
        setByte(bytes, offset + 3, (byte) (value >>> 24));
    }

    public static void setLong(byte[] bytes, int offset, long value) {
        setByte(bytes, offset, (byte) value);
        setByte(bytes, offset + 1, (byte) (value >>> 8));
        setByte(bytes, offset + 2, (byte) (value >>> 16));
        setByte(bytes, offset + 3, (byte) (value >>> 24));
        setByte(bytes, offset + 4, (byte) (value >>> 32));
        setByte(bytes, offset + 5, (byte) (value >>> 40));
        setByte(bytes, offset + 6, (byte) (value >>> 48));
        setByte(bytes, offset + 7, (byte) (value >>> 56));
    }

    public static void setShort(byte[] bytes, int offset, short value) {
        setByte(bytes, offset, (byte) value);
        setByte(bytes, offset + 1, (byte) (value >>> 8));
    }

    private UnsafeByteUtils() {
    }
}

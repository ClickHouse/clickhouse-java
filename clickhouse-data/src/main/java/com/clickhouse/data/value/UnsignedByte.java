package com.clickhouse.data.value;

import java.util.Locale;

/**
 * A wrapper class for unsigned {@code byte}.
 */
@Deprecated
public final class UnsignedByte extends Number implements Comparable<UnsignedByte> {
    public static final int BYTES = Byte.BYTES;

    public static final UnsignedByte ZERO;
    public static final UnsignedByte ONE;

    public static final UnsignedByte MIN_VALUE;
    public static final UnsignedByte MAX_VALUE;

    private static final int OFFSET = 0 - Byte.MIN_VALUE;
    private static final UnsignedByte[] CACHE = new UnsignedByte[OFFSET + Byte.MAX_VALUE + 1];

    static {
        for (int i = 0; i < CACHE.length; i++) {
            CACHE[i] = new UnsignedByte((byte) (i - OFFSET));
        }

        ZERO = CACHE[OFFSET];
        ONE = CACHE[OFFSET + 1];
        MIN_VALUE = ZERO;
        MAX_VALUE = CACHE[OFFSET - 1];
    }

    /**
     * Returns a {@code String} object representing the
     * specified unsigned byte.
     *
     * @param b an unsigned byte to be converted
     * @return a string representation of the argument in base&nbsp;10
     */
    public static String toString(byte b) {
        return Integer.toString(b >= 0 ? b : 0xFF & b);
    }

    /**
     * Returns a {@code UnsignedByte} instance representing the specified
     * {@code byte} value.
     *
     * @param b a byte value
     * @return a {@code UnsignedByte} instance representing {@code l}
     */
    public static UnsignedByte valueOf(byte b) {
        return CACHE[b + OFFSET];
    }

    /**
     * Returns a {@code UnsignedByte} object holding the value
     * of the specified {@code String}.
     *
     * @param s non-empty string
     * @return a {@code UnsignedByte} instance representing {@code s}
     */
    public static UnsignedByte valueOf(String s) {
        return valueOf(s, 10);
    }

    /**
     * Returns a {@code UnsignedByte} object holding the value
     * extracted from the specified {@code String} when parsed
     * with the radix given by the second argument.
     *
     * @param s     the {@code String} containing the unsigned integer
     *              representation to be parsed
     * @param radix the radix to be used while parsing {@code s}
     * @return the unsigned {@code byte} represented by the string
     *         argument in the specified radix
     * @throws NumberFormatException if the {@code String} does not contain a
     *                               parsable {@code byte}
     */
    public static UnsignedByte valueOf(String s, int radix) {
        int i = Integer.parseInt(s, radix);
        if (i < 0 || i > 0xFF) {
            throw new NumberFormatException(
                    String.format(Locale.ROOT, "String value %s exceeds range of unsigned byte.", s));
        }
        return valueOf((byte) i);
    }

    private final byte value;

    private UnsignedByte(byte value) {
        this.value = value;
    }

    /**
     * Returns an {@code UnsignedByte} whose value is {@code (this + val)}. If the
     * result would have more than 8 bits, returns the low 8 bits of the result.
     *
     * @param val value to be added to this unsigned byte, null is treated as zero
     * @return {@code this + val}
     */
    public UnsignedByte add(UnsignedByte val) {
        if (val == null || val.value == (byte) 0) {
            return this;
        }
        return valueOf((byte) (this.value + val.value));
    }

    /**
     * Returns an {@code UnsignedByte} whose value is {@code (this - val)}. If the
     * result would have more than 8 bits, returns the low 8 bits of the result.
     *
     * @param val value to be subtracted from this unsigned byte, null is treated as
     *            zero
     * @return {@code this - val}
     */
    public UnsignedByte subtract(UnsignedByte val) {
        if (val == null || val.value == (byte) 0) {
            return this;
        }
        return valueOf((byte) (this.value - val.value));
    }

    /**
     * Returns an {@code UnsignedByte} whose value is {@code (this * val)}. If the
     * result would have more than 8 bits, returns the low 8 bits of the result.
     *
     * @param val value to be multiplied by this unsigned byte, null is treated as
     *            zero
     * @return {@code this * val}
     */
    public UnsignedByte multiply(UnsignedByte val) {
        if (this.value == 0L || val == null || val.value == 0L) {
            return ZERO;
        }
        return valueOf((byte) (this.value * val.value));
    }

    /**
     * Returns an {@code UnsignedByte} whose value is {@code (this / val)}.
     *
     * @param val value by which this unsigned is to be divided, null is treated as
     *            zero
     * @return {@code this / val}
     * @throws ArithmeticException if {@code val} is null or zero.
     */
    public UnsignedByte divide(UnsignedByte val) {
        return valueOf((byte) Integer.divideUnsigned(0xFF & value, val == null ? 0 : 0xFF & val.value));
    }

    /**
     * Returns an {@code UnsignedByte} whose value is {@code (this % val)}.
     *
     * @param val value by which this unsigned byte is to be divided, and the
     *            remainder computed
     * @return {@code this % val}
     * @throws ArithmeticException if {@code val} is null or zero.
     */
    public UnsignedByte remainder(UnsignedByte val) {
        return valueOf((byte) Integer.remainderUnsigned(0xFF & value, val == null ? 0 : 0xFF & val.value));
    }

    @Override
    public int intValue() {
        return value >= 0 ? value : 0xFF & value;
    }

    @Override
    public long longValue() {
        return value >= 0 ? value : 0xFFL & value;
    }

    @Override
    public float floatValue() {
        return intValue();
    }

    @Override
    public double doubleValue() {
        return longValue();
    }

    @Override
    public int compareTo(UnsignedByte o) {
        return Integer.compareUnsigned(intValue(), o == null ? 0 : o.intValue());
    }

    @Override
    public int hashCode() {
        return 0xFF & value + 31;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        return value == ((UnsignedByte) obj).value;
    }

    @Override
    public String toString() {
        return Integer.toString(intValue());
    }

    /**
     * Returns a string representation of the first argument as an
     * unsigned integer value in the radix specified by the second
     * argument.
     *
     * @param radix the radix to use in the string representation
     * @return an unsigned string representation of the argument in the specified
     *         radix
     */
    public String toString(int radix) {
        return Integer.toString(intValue(), radix);
    }
}

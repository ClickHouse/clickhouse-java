package com.clickhouse.data.value;

import java.math.BigInteger;

/**
 * A wrapper class for unsigned {@code long}.
 */
@Deprecated
public final class UnsignedLong extends Number implements Comparable<UnsignedLong> {
    public static final BigInteger MASK = BigInteger.ONE.shiftLeft(Long.SIZE).subtract(BigInteger.ONE);

    public static final int BYTES = Long.BYTES;

    public static final UnsignedLong ZERO = new UnsignedLong(0L);
    public static final UnsignedLong ONE = new UnsignedLong(1L);
    public static final UnsignedLong TWO = new UnsignedLong(2L);
    public static final UnsignedLong TEN = new UnsignedLong(10L);

    public static final UnsignedLong MIN_VALUE = ZERO;
    public static final UnsignedLong MAX_VALUE = new UnsignedLong(-1L);

    /**
     * Returns a {@code UnsignedLong} instance representing the specified
     * {@code long} value.
     *
     * @param l a long value
     * @return a {@code UnsignedLong} instance representing {@code l}
     */
    public static UnsignedLong valueOf(long l) {
        if (l == -1L) {
            return MAX_VALUE;
        } else if (l == 0L) {
            return ZERO;
        } else if (l == 1L) {
            return ONE;
        } else if (l == 2L) {
            return TWO;
        } else if (l == 10L) {
            return TEN;
        }

        return new UnsignedLong(l);
    }

    /**
     * Returns a {@code UnsignedLong} instance representing the specified
     * {@code BigInteger} value.
     *
     * @param val a BigInteger value, null is treated as zero
     * @return a {@code UnsignedLong} instance representing {@code val}
     */
    public static UnsignedLong valueOf(BigInteger val) {
        return val == null ? ZERO : valueOf(val.longValue());
    }

    /**
     * Returns a {@code UnsignedLong} object holding the value
     * of the specified {@code String}.
     *
     * @param s non-empty string
     * @return a {@code UnsignedLong} instance representing {@code s}
     */
    public static UnsignedLong valueOf(String s) {
        return valueOf(s, 10);
    }

    /**
     * Returns a {@code UnsignedLong} object holding the value
     * extracted from the specified {@code String} when parsed
     * with the radix given by the second argument.
     *
     * @param s     the {@code String} containing the unsigned integer
     *              representation to be parsed
     * @param radix the radix to be used while parsing {@code s}
     * @return the unsigned {@code long} represented by the string
     *         argument in the specified radix
     * @throws NumberFormatException if the {@code String} does not contain a
     *                               parsable {@code long}
     */
    public static UnsignedLong valueOf(String s, int radix) {
        return valueOf(Long.parseUnsignedLong(s, radix));
    }

    private final long value;

    private UnsignedLong(long value) {
        this.value = value;
    }

    /**
     * Returns an {@code UnsignedLong} whose value is {@code (this + val)}. If the
     * result would have more than 64 bits, returns the low 64 bits of the result.
     *
     * @param val value to be added to this unsigned long, null is treated as zero
     * @return {@code this + val}
     */
    public UnsignedLong add(UnsignedLong val) {
        if (val == null || val.value == 0L) {
            return this;
        }
        return valueOf(this.value + val.value);
    }

    /**
     * Returns an {@code UnsignedLong} whose value is {@code (this - val)}. If the
     * result would have more than 64 bits, returns the low 64 bits of the result.
     *
     * @param val value to be subtracted from this unsigned long, null is treated as
     *            zero
     * @return {@code this - val}
     */
    public UnsignedLong subtract(UnsignedLong val) {
        if (val == null || val.value == 0L) {
            return this;
        }
        return valueOf(this.value - val.value);
    }

    /**
     * Returns an {@code UnsignedLong} whose value is {@code (this * val)}. If the
     * result would have more than 64 bits, returns the low 64 bits of the result.
     *
     * @param val value to be multiplied by this unsigned long, null is treated as
     *            zero
     * @return {@code this * val}
     */
    public UnsignedLong multiply(UnsignedLong val) {
        if (this.value == 0L || val == null || val.value == 0L) {
            return ZERO;
        }
        return valueOf(this.value * val.value);
    }

    /**
     * Returns an {@code UnsignedLong} whose value is {@code (this / val)}.
     *
     * @param val value by which this unsigned is to be divided, null is treated as
     *            zero
     * @return {@code this / val}
     * @throws ArithmeticException if {@code val} is null or zero.
     */
    public UnsignedLong divide(UnsignedLong val) {
        return valueOf(Long.divideUnsigned(this.value, val == null ? 0L : val.value));
    }

    /**
     * Returns an {@code UnsignedLong} whose value is {@code (this % val)}.
     *
     * @param val value by which this unsigned long is to be divided, and the
     *            remainder computed
     * @return {@code this % val}
     * @throws ArithmeticException if {@code val} is null or zero.
     */
    public UnsignedLong remainder(UnsignedLong val) {
        return valueOf(Long.remainderUnsigned(value, val == null ? 0L : val.value));
    }

    @Override
    public int intValue() {
        return (int) value;
    }

    @Override
    public long longValue() {
        return value;
    }

    /**
     * Returns the value of this {@code UnsignedLong} as a {@link BigInteger}.
     *
     * @return a BigInteger representing the unsigned long value
     */
    public BigInteger bigIntegerValue() {
        BigInteger v = BigInteger.valueOf(value);
        if (value < 0L) {
            v = v.and(MASK);
        }
        return v;
    }

    @Override
    public float floatValue() {
        if (value >= 0) {
            return value;
        }
        // https://github.com/google/guava/blob/v31.1/guava/src/com/google/common/primitives/UnsignedLong.java#L202
        return ((value >>> 1) | (value & 1L)) * 2f;
    }

    @Override
    public double doubleValue() {
        if (value >= 0) {
            return value;
        }
        // https://github.com/google/guava/blob/v31.1/guava/src/com/google/common/primitives/UnsignedLong.java#L216
        return ((value >>> 1) | (value & 1L)) * 2.0;
    }

    @Override
    public int compareTo(UnsignedLong o) {
        return Long.compareUnsigned(value, o == null ? 0L : o.value);
    }

    @Override
    public int hashCode() {
        return 31 + (int) (value ^ (value >>> 32));
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        return value == ((UnsignedLong) obj).value;
    }

    @Override
    public String toString() {
        return Long.toUnsignedString(value);
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
        return Long.toUnsignedString(value, radix);
    }
}
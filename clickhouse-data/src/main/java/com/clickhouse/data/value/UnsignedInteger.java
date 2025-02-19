package com.clickhouse.data.value;

/**
 * A wrapper class for unsigned {@code int}.
 */
@Deprecated
public final class UnsignedInteger extends Number implements Comparable<UnsignedInteger> {
    public static final int BYTES = Integer.BYTES;

    public static final UnsignedInteger ZERO = new UnsignedInteger(0);
    public static final UnsignedInteger ONE = new UnsignedInteger(1);
    public static final UnsignedInteger TWO = new UnsignedInteger(2);
    public static final UnsignedInteger TEN = new UnsignedInteger(10);

    public static final UnsignedInteger MIN_VALUE = ZERO;
    public static final UnsignedInteger MAX_VALUE = new UnsignedInteger(-1);

    /**
     * Returns a {@code UnsignedInteger} instance representing the specified
     * {@code int} value.
     *
     * @param i an int value
     * @return a {@code UnsignedInteger} instance representing {@code l}
     */
    public static UnsignedInteger valueOf(int i) {
        if (i == -1L) {
            return MAX_VALUE;
        } else if (i == 0L) {
            return ZERO;
        } else if (i == 1L) {
            return ONE;
        } else if (i == 2L) {
            return TWO;
        } else if (i == 10L) {
            return TEN;
        }

        return new UnsignedInteger(i);
    }

    /**
     * Returns a {@code UnsignedInteger} object holding the value
     * of the specified {@code String}.
     *
     * @param s non-empty string
     * @return a {@code UnsignedInteger} instance representing {@code s}
     */
    public static UnsignedInteger valueOf(String s) {
        return valueOf(s, 10);
    }

    /**
     * Returns a {@code UnsignedInteger} object holding the value
     * extracted from the specified {@code String} when parsed
     * with the radix given by the second argument.
     *
     * @param s     the {@code String} containing the unsigned integer
     *              representation to be parsed
     * @param radix the radix to be used while parsing {@code s}
     * @return the unsigned {@code long} represented by the string
     *         argument in the specified radix
     * @throws NumberFormatException if the {@code String} does not contain a
     *                               parsable {@code int}
     */
    public static UnsignedInteger valueOf(String s, int radix) {
        return valueOf(Integer.parseUnsignedInt(s, radix));
    }

    private final int value;

    private UnsignedInteger(int value) {
        this.value = value;
    }

    /**
     * Returns an {@code UnsignedInteger} whose value is {@code (this + val)}. If
     * the
     * result would have more than 32 bits, returns the low 32 bits of the result.
     *
     * @param val value to be added to this unsigned integer, null is treated as
     *            zero
     * @return {@code this + val}
     */
    public UnsignedInteger add(UnsignedInteger val) {
        if (val == null || val.value == 0L) {
            return this;
        }
        return valueOf(this.value + val.value);
    }

    /**
     * Returns an {@code UnsignedInteger} whose value is {@code (this - val)}. If
     * the
     * result would have more than 32 bits, returns the low 32 bits of the result.
     *
     * @param val value to be subtracted from this unsigned integer, null is treated
     *            as
     *            zero
     * @return {@code this - val}
     */
    public UnsignedInteger subtract(UnsignedInteger val) {
        if (val == null || val.value == 0L) {
            return this;
        }
        return valueOf(this.value - val.value);
    }

    /**
     * Returns an {@code UnsignedInteger} whose value is {@code (this * val)}. If
     * the
     * result would have more than 32 bits, returns the low 32 bits of the result.
     *
     * @param val value to be multiplied by this unsigned integer, null is treated
     *            as
     *            zero
     * @return {@code this * val}
     */
    public UnsignedInteger multiply(UnsignedInteger val) {
        if (this.value == 0L || val == null || val.value == 0L) {
            return ZERO;
        }
        return valueOf(this.value * val.value);
    }

    /**
     * Returns an {@code UnsignedInteger} whose value is {@code (this / val)}.
     *
     * @param val value by which this unsigned is to be divided, null is treated as
     *            zero
     * @return {@code this / val}
     * @throws ArithmeticException if {@code val} is null or zero.
     */
    public UnsignedInteger divide(UnsignedInteger val) {
        return valueOf(Integer.divideUnsigned(this.value, val == null ? 0 : val.value));
    }

    /**
     * Returns an {@code UnsignedInteger} whose value is {@code (this % val)}.
     *
     * @param val value by which this unsigned integer is to be divided, and the
     *            remainder computed
     * @return {@code this % val}
     * @throws ArithmeticException if {@code val} is null or zero.
     */
    public UnsignedInteger remainder(UnsignedInteger val) {
        return valueOf(Integer.remainderUnsigned(value, val == null ? 0 : val.value));
    }

    @Override
    public int intValue() {
        return value;
    }

    @Override
    public long longValue() {
        return value >= 0 ? value : 0xFFFFFFFFL & value;
    }

    @Override
    public float floatValue() {
        return longValue();
    }

    @Override
    public double doubleValue() {
        return longValue();
    }

    @Override
    public int compareTo(UnsignedInteger o) {
        return Integer.compareUnsigned(value, o == null ? 0 : o.value);
    }

    @Override
    public int hashCode() {
        return 31 + value;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        return value == ((UnsignedInteger) obj).value;
    }

    @Override
    public String toString() {
        return Integer.toUnsignedString(value);
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
        return Integer.toUnsignedString(value, radix);
    }
}
package com.clickhouse.data.value;

import java.util.Locale;

/**
 * A wrapper class for unsigned {@code short}.
 */
@Deprecated
public final class UnsignedShort extends Number implements Comparable<UnsignedShort> {
    public static final int BYTES = Short.BYTES;

    public static final UnsignedShort ZERO = new UnsignedShort((short) 0);
    public static final UnsignedShort ONE = new UnsignedShort((short) 1);

    public static final UnsignedShort MIN_VALUE = ZERO;
    public static final UnsignedShort MAX_VALUE = new UnsignedShort((short) -1);

    /**
     * Returns a {@code String} object representing the
     * specified unsigned short.
     *
     * @param s an unsigned short to be converted
     * @return a string representation of the argument in base&nbsp;10
     */
    public static String toString(short s) {
        return Integer.toString(s >= 0 ? s : 0xFFFF & s);
    }

    /**
     * Returns a {@code UnsignedShort} instance representing the specified
     * {@code short} value.
     *
     * @param s a short value
     * @return a {@code UnsignedShort} instance representing {@code l}
     */
    public static UnsignedShort valueOf(short s) {
        if (s == (short) -1) {
            return MAX_VALUE;
        } else if (s == (short) 0) {
            return ZERO;
        } else if (s == (short) 1) {
            return ONE;
        }
        return new UnsignedShort(s);
    }

    /**
     * Returns a {@code UnsignedShort} object holding the value
     * of the specified {@code String}.
     *
     * @param s non-empty string
     * @return a {@code UnsignedShort} instance representing {@code s}
     */
    public static UnsignedShort valueOf(String s) {
        return valueOf(s, 10);
    }

    /**
     * Returns a {@code UnsignedShort} object holding the value
     * extracted from the specified {@code String} when parsed
     * with the radix given by the second argument.
     *
     * @param s     the {@code String} containing the unsigned integer
     *              representation to be parsed
     * @param radix the radix to be used while parsing {@code s}
     * @return the unsigned {@code short} represented by the string
     *         argument in the specified radix
     * @throws NumberFormatException if the {@code String} does not contain a
     *                               parsable {@code short}
     */
    public static UnsignedShort valueOf(String s, int radix) {
        int i = Integer.parseInt(s, radix);
        if (i < 0 || i > 0xFFFF) {
            throw new NumberFormatException(
                    String.format(Locale.ROOT, "String value %s exceeds range of unsigned short.", s));
        }
        return valueOf((short) Integer.parseInt(s, radix));
    }

    private final short value;

    private UnsignedShort(short value) {
        this.value = value;
    }

    /**
     * Returns an {@code UnsignedShort} whose value is {@code (this + val)}. If the
     * result would have more than 16 bits, returns the low 16 bits of the result.
     *
     * @param val value to be added to this unsigned short, null is treated as zero
     * @return {@code this + val}
     */
    public UnsignedShort add(UnsignedShort val) {
        if (val == null || val.value == (short) 0) {
            return this;
        }
        return valueOf((short) (this.value + val.value));
    }

    /**
     * Returns an {@code UnsignedShort} whose value is {@code (this - val)}. If the
     * result would have more than 16 bits, returns the low 16 bits of the result.
     *
     * @param val value to be subtracted from this unsigned short, null is treated
     *            as zero
     * @return {@code this - val}
     */
    public UnsignedShort subtract(UnsignedShort val) {
        if (val == null || val.value == (short) 0) {
            return this;
        }
        return valueOf((short) (this.value - val.value));
    }

    /**
     * Returns an {@code UnsignedShort} whose value is {@code (this * val)}. If the
     * result would have more than 16 bits, returns the low 16 bits of the result.
     *
     * @param val value to be multiplied by this unsigned short, null is treated as
     *            zero
     * @return {@code this * val}
     */
    public UnsignedShort multiply(UnsignedShort val) {
        if (this.value == 0L || val == null || val.value == 0L) {
            return ZERO;
        }
        return valueOf((short) (this.value * val.value));
    }

    /**
     * Returns an {@code UnsignedShort} whose value is {@code (this / val)}.
     *
     * @param val value by which this unsigned is to be divided, null is treated as
     *            zero
     * @return {@code this / val}
     * @throws ArithmeticException if {@code val} is null or zero.
     */
    public UnsignedShort divide(UnsignedShort val) {
        return valueOf((short) Integer.divideUnsigned(intValue(), val == null ? 0 : val.intValue()));
    }

    /**
     * Returns an {@code UnsignedShort} whose value is {@code (this % val)}.
     *
     * @param val value by which this unsigned short is to be divided, and the
     *            remainder computed
     * @return {@code this % val}
     * @throws ArithmeticException if {@code val} is null or zero.
     */
    public UnsignedShort remainder(UnsignedShort val) {
        return valueOf((short) Integer.remainderUnsigned(intValue(), val == null ? 0 : val.intValue()));
    }

    @Override
    public int intValue() {
        return value >= 0 ? value : 0xFFFF & value;
    }

    @Override
    public long longValue() {
        return value >= 0 ? value : 0xFFFFL & value;
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
    public int compareTo(UnsignedShort o) {
        return Integer.compareUnsigned(intValue(), o == null ? 0 : o.intValue());
    }

    @Override
    public int hashCode() {
        return 0xFFFF & value + 31;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        return value == ((UnsignedShort) obj).value;
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

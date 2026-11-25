package com.clickhouse.client.cli;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;

/**
 * Writes floating-point values as ClickHouse does: the shortest text that reads back as the same value, in
 * positional notation when the decimal exponent is in [-6, 21), else in scientific notation without a {@code +}
 * sign or exponent padding ({@code 1e21}, {@code 1.5e-7}). This is the same layout as JavaScript's
 * {@code Number.prototype.toString}.
 *
 * <p>{@link Double#toString(double)} is not a substitute: before JDK 19 it sometimes gives more digits than
 * necessary, and it uses another layout. The digits come from the exact decimal value of the binary value
 * ({@link BigDecimal}), rounded to the smallest precision that reads back as the same value.
 */
final class FloatFormat {

    private FloatFormat() {
    }

    static String formatDouble(double f) {
        String special = special(f, Double.isNaN(f), Double.isInfinite(f), f == 0, 1 / f < 0);
        if (special != null) {
            return special;
        }
        BigDecimal exact = new BigDecimal(Math.abs(f));
        int hi = significantDigits(Double.toString(Math.abs(f)));
        BigDecimal shortest = shortest(exact, hi, 17, p -> Double.parseDouble(p.toString()) == Math.abs(f));
        return layout(f < 0, shortest);
    }

    static String formatFloat(float f) {
        String special = special(f, Float.isNaN(f), Float.isInfinite(f), f == 0, 1 / f < 0);
        if (special != null) {
            return special;
        }
        float abs = Math.abs(f);
        BigDecimal exact = new BigDecimal(abs);
        int hi = significantDigits(Float.toString(abs));
        BigDecimal shortest = shortest(exact, hi, 9, p -> Float.parseFloat(p.toString()) == abs);
        return layout(f < 0, shortest);
    }

    private static String special(double f, boolean nan, boolean inf, boolean zero, boolean negZero) {
        if (nan) {
            return "nan";
        }
        if (inf) {
            return f > 0 ? "inf" : "-inf";
        }
        if (zero) {
            return negZero ? "-0" : "0";
        }
        return null;
    }

    private interface RoundTrip {
        boolean test(BigDecimal candidate);
    }

    /**
     * Returns the exact value rounded to the smallest precision that reads back as the same value. Rounding to
     * more digits only comes closer to the value, so the round-trip test is monotonic in the precision.
     * {@code hint} (the digit count of {@code Double.toString}, which reads back) bounds the search.
     */
    private static BigDecimal shortest(BigDecimal exact, int hint, int max, RoundTrip roundTrip) {
        int hi = Math.max(1, Math.min(hint, max));
        BigDecimal best = round(exact, hi);
        if (!roundTrip.test(best)) {
            hi = max;
            best = round(exact, hi);
        }
        int lo = 1;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            BigDecimal candidate = round(exact, mid);
            if (roundTrip.test(candidate)) {
                hi = mid;
                best = candidate;
            } else {
                lo = mid + 1;
            }
        }
        return best;
    }

    private static BigDecimal round(BigDecimal exact, int precision) {
        return exact.round(new MathContext(precision, RoundingMode.HALF_EVEN));
    }

    /** Counts the significant digits of the text of Double.toString or Float.toString. */
    private static int significantDigits(String s) {
        int e = s.indexOf('E');
        String mantissa = e >= 0 ? s.substring(0, e) : s;
        String digits = mantissa.replace(".", "");
        int start = 0;
        while (start < digits.length() - 1 && digits.charAt(start) == '0') {
            start++;
        }
        int end = digits.length();
        while (end > start + 1 && digits.charAt(end - 1) == '0') {
            end--;
        }
        return end - start;
    }

    /** Writes the decimal value v (positive) in the layout of JavaScript's Number.prototype.toString. */
    private static String layout(boolean negative, BigDecimal v) {
        BigDecimal stripped = v.stripTrailingZeros();
        String digits = stripped.unscaledValue().toString();
        // The value is 0.digits x 10^n.
        int k = digits.length();
        int n = k - stripped.scale();
        StringBuilder out = new StringBuilder(k + 8);
        if (negative) {
            out.append('-');
        }
        if (k <= n && n <= 21) {
            out.append(digits);
            for (int i = 0; i < n - k; i++) {
                out.append('0');
            }
        } else if (0 < n && n <= 21) {
            out.append(digits, 0, n).append('.').append(digits, n, k);
        } else if (-6 < n && n <= 0) {
            out.append("0.");
            for (int i = 0; i < -n; i++) {
                out.append('0');
            }
            out.append(digits);
        } else {
            out.append(digits.charAt(0));
            if (k > 1) {
                out.append('.').append(digits, 1, k);
            }
            out.append('e').append(n - 1);
        }
        return out.toString();
    }
}

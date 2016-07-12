package ru.yandex.clickhouse.response;


public final class ByteFragmentUtils {

    private ByteFragmentUtils() {
    }

    public static int parseInt(ByteFragment s) throws NumberFormatException {
        if (s == null) {
            throw new NumberFormatException("null");
        }

        if (s.isNull()) {
            return 0; //jdbc spec
        }

        int result = 0;
        boolean negative = false;
        int i = 0, max = s.length();
        int limit;
        int multmin;
        int digit;

        if (max > 0) {
            if (s.charAt(0) == '-') {
                negative = true;
                limit = Integer.MIN_VALUE;
                i++;
            } else {
                limit = -Integer.MAX_VALUE;
            }
            multmin = limit / 10;
            if (i < max) {
                digit = s.charAt(i++) - 0x30; //Character.digit(s.charAt(i++), 10);
                if (digit < 0) {
                    throw new NumberFormatException("For input string: \"" + s.asString() + '"');
                } else {
                    result = -digit;
                }
            }
            while (i < max) {
                // Accumulating negatively avoids surprises near MAX_VALUE
                digit = s.charAt(i++) - 0x30; // Character.digit(s.charAt(i++), 10);
                if (digit < 0 || digit > 9) {
                    throw new NumberFormatException("For input string: \"" + s.asString() + '"');
                }
                if (result < multmin) {
                    throw new NumberFormatException("For input string: \"" + s.asString() + '"');
                }
                result *= 10;
                if (result < limit + digit) {
                    throw new NumberFormatException("For input string: \"" + s.asString() + '"');
                }
                result -= digit;
            }
        } else {
            throw new NumberFormatException("For input string: \"" + s.asString() + '"');
        }
        if (negative) {
            if (i > 1) {
                return result;
            } else {    /* Only got "-" */
                throw new NumberFormatException("For input string: \"" + s.asString() + '"');
            }
        } else {
            return -result;
        }
    }


    public static long parseLong(ByteFragment s) throws NumberFormatException {
        if (s == null) {
            throw new NumberFormatException("null");
        }

        if (s.isNull()) {
            return 0; //jdbc spec
        }

        long result = 0;
        boolean negative = false;
        int i = 0, max = s.length();
        long limit;
        long multmin;
        int digit;

        if (max > 0) {
            if (s.charAt(0) == '-') {
                negative = true;
                limit = Long.MIN_VALUE;
                i++;
            } else {
                limit = -Long.MAX_VALUE;
            }
            multmin = limit / 10;
            if (i < max) {
                digit = s.charAt(i++) - 0x30; // Character.digit(s.charAt(i++), 10);
                if (digit < 0 || digit > 9) {
                    throw new NumberFormatException("For input string: \"" + s.asString() + '"');
                } else {
                    result = -digit;
                }
            }
            while (i < max) {
                // Accumulating negatively avoids surprises near MAX_VALUE
                digit = s.charAt(i++) - 0x30; // Character.digit(s.charAt(i++), 10);
                if (digit < 0 || digit > 9) {
                    throw new NumberFormatException("For input string: \"" + s.asString() + '"');
                }
                if (result < multmin) {
                    throw new NumberFormatException("For input string: \"" + s.asString() + '"');
                }
                result *= 10;
                if (result < limit + digit) {
                    throw new NumberFormatException("For input string: \"" + s.asString() + '"');
                }
                result -= digit;
            }
        } else {
            throw new NumberFormatException("For input string: \"" + s.asString() + '"');
        }
        if (negative) {
            if (i > 1) {
                return result;
            } else {    /* Only got "-" */
                throw new NumberFormatException("For input string: \"" + s.asString() + '"');
            }
        } else {
            return -result;
        }
    }
}

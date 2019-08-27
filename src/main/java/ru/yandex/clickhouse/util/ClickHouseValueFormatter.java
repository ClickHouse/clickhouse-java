package ru.yandex.clickhouse.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.TimeZone;
import java.util.UUID;

import ru.yandex.clickhouse.ClickHouseArray;
import ru.yandex.clickhouse.ClickHouseUtil;

public final class ClickHouseValueFormatter {

    public static final String NULL_MARKER  = "\\N";

    private static ThreadLocal<SimpleDateFormat> dateFormat = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd");
        }
    };

    private static ThreadLocal<SimpleDateFormat> dateTimeFormat = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }
    };

    public static String formatBytes(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        char[] hexArray =
            {'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F'};
        char[] hexChars = new char[bytes.length * 4];
        int v;
        for ( int j = 0; j < bytes.length; j++ ) {
            v = bytes[j] & 0xFF;
            hexChars[j * 4]     = '\\';
            hexChars[j * 4 + 1] = 'x';
            hexChars[j * 4 + 2] = hexArray[v/16];
            hexChars[j * 4 + 3] = hexArray[v%16];
        }
        return new String(hexChars);
    }

    public static String formatInt(int myInt) {
        return Integer.toString(myInt);
    }

    public static String formatDouble(double myDouble) {
        return Double.toString(myDouble);
    }

    public static String formatChar(char myChar) {
        return Character.toString(myChar);
    }

    public static String formatLong(long myLong) {
        return Long.toString(myLong);
    }

    public static String formatFloat(float myFloat) {
        return Float.toString(myFloat);
    }

    public static String formatBigDecimal(BigDecimal myBigDecimal) {
        return myBigDecimal != null ? myBigDecimal.toPlainString() : NULL_MARKER;
    }

    public static String formatShort(short myShort) {
        return Short.toString(myShort);
    }

    public static String formatString(String myString) {
        return ClickHouseUtil.escape(myString);
    }

    public static String formatNull() {
        return NULL_MARKER;
    }

    public static String formatByte(byte myByte) {
        return Byte.toString(myByte);
    }

    public static String formatBoolean(boolean myBoolean) {
        return myBoolean ? "1" : "0";
    }

    public static String formatDate(Date date, TimeZone timeZone) {
        getDateFormat().setTimeZone(timeZone);
        return getDateFormat().format(date);
    }

    public static String formatTime(Time time, TimeZone timeZone) {
        getDateTimeFormat().setTimeZone(timeZone);
        return getDateTimeFormat().format(time);
    }

    public static String formatTimestamp(Timestamp time, TimeZone timeZone) {
        getDateTimeFormat().setTimeZone(timeZone);
        return getDateTimeFormat().format(time);
    }

    public static String formatUUID(UUID x) {
        return x.toString();
    }

    public static String formatBigInteger(BigInteger x) {
        return x.toString();
    }

    public static String formatObject(Object x, TimeZone dateTimeZone,
        TimeZone dateTimeTimeZone)
    {
        if (x == null) {
            return null;
        }
        if (x instanceof Byte) {
            return formatInt(((Byte) x).intValue());
        } else if (x instanceof String) {
            return formatString((String) x);
        } else if (x instanceof BigDecimal) {
            return formatBigDecimal((BigDecimal) x);
        } else if (x instanceof Short) {
            return formatShort(((Short) x).shortValue());
        } else if (x instanceof Integer) {
            return formatInt(((Integer) x).intValue());
        } else if (x instanceof Long) {
            return formatLong(((Long) x).longValue());
        } else if (x instanceof Float) {
            return formatFloat(((Float) x).floatValue());
        } else if (x instanceof Double) {
            return formatDouble(((Double) x).doubleValue());
        } else if (x instanceof byte[]) {
            return formatBytes((byte[]) x);
        } else if (x instanceof Date) {
            return formatDate((Date) x, dateTimeZone);
        } else if (x instanceof Time) {
            return formatTime((Time) x, dateTimeTimeZone);
        } else if (x instanceof Timestamp) {
            return formatTimestamp((Timestamp) x, dateTimeTimeZone);
        } else if (x instanceof Boolean) {
            return formatBoolean(((Boolean) x).booleanValue());
        } else if (x instanceof UUID) {
            return formatUUID((UUID) x);
        } else if (x instanceof BigInteger) {
            return formatBigInteger((BigInteger) x);
        } else if (x instanceof Collection) {
            return ClickHouseArrayUtil.toString((Collection) x, dateTimeZone, dateTimeTimeZone);
        } else if (x.getClass().isArray()) {
            return ClickHouseArrayUtil.arrayToString(x, dateTimeZone, dateTimeTimeZone);
        } else {
            return String.valueOf(x);
        }
    }

    public static boolean needsQuoting(Object o) {
        if (o == null) {
            return false;
        }
        if (o instanceof Number) {
            return false;
        }
        if (o.getClass().isArray()) {
            return false;
        }
        if (o instanceof ClickHouseArray) {
            return false;
        }
        return true;
    }

    private static SimpleDateFormat getDateFormat() {
        return dateFormat.get();
    }

    private static SimpleDateFormat getDateTimeFormat() {
        return dateTimeFormat.get();
    }

    private ClickHouseValueFormatter() { /* NOP */ }
}

package com.clickhouse.client.cli;

import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.data_formats.internal.StringValue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.Period;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Writes the Java values that the client returns as the text that the server writes for the {@code TabSeparated}
 * format. The upstream {@code .reference} files hold the server's text, so a test passes only when the client
 * decoded every value correctly.
 *
 * <p>The rendering is type-directed: the column type from the result metadata selects the text form, because the
 * Java value alone is not sufficient (for example, the precision of a {@code DateTime64} is in the type only).
 * The renderer accepts the Java types of both backends (client-v2 {@code readValue} and jdbc-v2
 * {@code getObject}); any other Java type for a column type is an error, so a test cannot pass by accident.
 *
 * <p>ClickHouse uses two text forms:
 * <ul>
 *   <li>top level ({@code serializeTextEscaped}): strings are escaped, not quoted, and NULL is {@code \N};</li>
 *   <li>nested in {@code Array}, {@code Tuple} and {@code Map} ({@code serializeTextQuoted}): strings are quoted,
 *       and NULL is {@code NULL}.</li>
 * </ul>
 */
final class TsvRenderer {

    /** A column type that the renderer does not support. The statement fails. */
    static final class UnsupportedTypeException extends RuntimeException {
        UnsupportedTypeException(String type) {
            super("clickhouse-client-cli: no TabSeparated renderer for type " + type);
        }
    }

    /** A Java value that does not fit the column type. The statement fails. */
    static final class ValueMismatchException extends RuntimeException {
        ValueMismatchException(ChType type, Object value) {
            super("clickhouse-client-cli: unexpected Java value " + describe(value) + " for type " + type);
        }

        private static String describe(Object value) {
            String text = String.valueOf(value);
            if (text.length() > 80) {
                text = text.substring(0, 80) + "...";
            }
            return value.getClass().getName() + " (" + text + ")";
        }
    }

    private final ChType type;

    TsvRenderer(String typeName) {
        this.type = ChType.parse(typeName);
    }

    /**
     * Writes one top-level value. With raw (the {@code TSVRaw} formats), a top-level string is written without
     * escapes.
     */
    void render(TsvOutput out, Object value, boolean raw) {
        if (raw && value != null && isStringType(type)) {
            byte[] bytes = stringBytes(type, value);
            if (bytes != null) {
                out.write(bytes);
                return;
            }
        }
        renderValue(out, type, value, false);
    }

    static void renderValue(TsvOutput out, ChType t, Object v, boolean nested) {
        switch (t.base) {
            case "Nullable":
            case "LowCardinality": {
                ChType inner = t.typeArg(0);
                if (inner == null) {
                    throw new UnsupportedTypeException(t.toString());
                }
                renderValue(out, inner, v, nested);
                return;
            }
            case "SimpleAggregateFunction": {
                ChType inner = t.typeArg(t.args.size() - 1);
                if (inner == null) {
                    throw new UnsupportedTypeException(t.toString());
                }
                renderValue(out, inner, v, nested);
                return;
            }
            default:
                break;
        }
        if (v == null) {
            out.write(nested ? "NULL" : "\\N");
            return;
        }
        switch (t.base) {
            case "Int8":
            case "Int16":
            case "Int32":
            case "Int64":
            case "Int128":
            case "Int256":
            case "UInt8":
            case "UInt16":
            case "UInt32":
            case "UInt64":
            case "UInt128":
            case "UInt256":
                if (v instanceof Byte || v instanceof Short || v instanceof Integer || v instanceof Long
                        || v instanceof BigInteger) {
                    out.write(v.toString());
                    return;
                }
                throw new ValueMismatchException(t, v);
            case "Float32":
            case "BFloat16":
                if (v instanceof Float) {
                    out.write(FloatFormat.formatFloat((Float) v));
                    return;
                }
                throw new ValueMismatchException(t, v);
            case "Float64":
                if (v instanceof Double) {
                    out.write(FloatFormat.formatDouble((Double) v));
                    return;
                }
                throw new ValueMismatchException(t, v);
            case "Bool":
                if (v instanceof Boolean) {
                    out.write(v.toString());
                    return;
                }
                throw new ValueMismatchException(t, v);
            case "String":
            case "FixedString":
            case "Enum8":
            case "Enum16":
            case "Enum": {
                byte[] bytes = stringBytes(t, v);
                if (bytes == null) {
                    throw new ValueMismatchException(t, v);
                }
                writeEscaped(out, bytes, nested);
                return;
            }
            case "Decimal":
            case "Decimal32":
            case "Decimal64":
            case "Decimal128":
            case "Decimal256":
                if (v instanceof BigDecimal) {
                    out.write(formatDecimal((BigDecimal) v));
                    return;
                }
                throw new ValueMismatchException(t, v);
            case "Date":
            case "Date32":
                quotedIf(out, formatDate(t, v), nested);
                return;
            case "DateTime":
            case "DateTime32":
                quotedIf(out, formatDateTime(t, v, 0), nested);
                return;
            case "DateTime64": {
                int precision = precision(t, 0);
                quotedIf(out, formatDateTime(t, v, precision), nested);
                return;
            }
            case "Time":
                quotedIf(out, formatTime(t, v, 0), nested);
                return;
            case "Time64":
                quotedIf(out, formatTime(t, v, precision(t, 0)), nested);
                return;
            case "UUID":
                if (v instanceof UUID) {
                    quotedIf(out, v.toString(), nested);
                    return;
                }
                throw new ValueMismatchException(t, v);
            case "IPv4":
                if (v instanceof Inet4Address) {
                    quotedIf(out, ((Inet4Address) v).getHostAddress(), nested);
                    return;
                }
                throw new ValueMismatchException(t, v);
            case "IPv6":
                if (v instanceof Inet6Address) {
                    quotedIf(out, formatIPv6(((Inet6Address) v).getAddress()), nested);
                    return;
                }
                if (v instanceof Inet4Address) {
                    // InetAddress.getByAddress gives an IPv4-mapped address (::ffff:a.b.c.d) as Inet4Address.
                    byte[] b = new byte[16];
                    b[10] = (byte) 0xff;
                    b[11] = (byte) 0xff;
                    System.arraycopy(((InetAddress) v).getAddress(), 0, b, 12, 4);
                    quotedIf(out, formatIPv6(b), nested);
                    return;
                }
                throw new ValueMismatchException(t, v);
            case "Nothing":
                // A `Nothing` column has no values; a value here is a NULL.
                out.write(nested ? "NULL" : "\\N");
                return;
            case "Array": {
                ChType inner = t.typeArg(0);
                List<?> items = sequence(v);
                if (inner == null || items == null) {
                    throw new ValueMismatchException(t, v);
                }
                renderSequence(out, inner, items, '[', ']');
                return;
            }
            case "Nested": {
                // Nested(a T, b U) (with flatten_nested = 0) is Array(Tuple(a T, b U)).
                List<?> items = sequence(v);
                if (items == null) {
                    throw new ValueMismatchException(t, v);
                }
                renderSequence(out, new ChType("Tuple", t.args), items, '[', ']');
                return;
            }
            case "Tuple":
                renderTuple(out, t, v);
                return;
            case "Map":
                renderMap(out, t, v);
                return;
            case "Point":
                renderPoint(out, t, v);
                return;
            case "Ring":
            case "LineString":
            case "MultiPoint":
                renderGeo(out, t, v, ChType.of("Point"));
                return;
            case "Polygon":
            case "MultiLineString":
                renderGeo(out, t, v, ChType.of("Ring"));
                return;
            case "MultiPolygon":
                renderGeo(out, t, v, ChType.of("Polygon"));
                return;
            case "Geometry":
                renderGeometry(out, t, v);
                return;
            case "Variant":
                renderVariant(out, t, v, nested);
                return;
            case "QBit": {
                // QBit(T, dimension) is written as an array of its element type.
                ChType inner = t.typeArg(0);
                List<?> items = sequence(v);
                if (inner == null || items == null) {
                    throw new ValueMismatchException(t, v);
                }
                renderSequence(out, inner, items, '[', ']');
                return;
            }
            default:
                if (t.base.startsWith("Interval")) {
                    out.write(formatInterval(t, v));
                    return;
                }
                throw new UnsupportedTypeException(t.toString());
        }
    }

    static boolean isStringType(ChType t) {
        while ((t.base.equals("Nullable") || t.base.equals("LowCardinality")) && t.typeArg(0) != null) {
            t = t.typeArg(0);
        }
        switch (t.base) {
            case "String":
            case "FixedString":
            case "Enum8":
            case "Enum16":
            case "Enum":
                return true;
            default:
                return false;
        }
    }

    /** Returns the bytes of a string-like value, or null when v is not one. */
    private static byte[] stringBytes(ChType t, Object v) {
        if (v instanceof String) {
            return ((String) v).getBytes(StandardCharsets.UTF_8);
        }
        if (v instanceof StringValue) {
            return ((StringValue) v).toByteArray();
        }
        if (v instanceof BinaryStreamReader.EnumValue && t.base.startsWith("Enum")) {
            return ((BinaryStreamReader.EnumValue) v).getName().getBytes(StandardCharsets.UTF_8);
        }
        return null;
    }

    /**
     * Writes s with the escapes of ClickHouse's {@code writeAnyEscapedString} (with the single quote as the quote
     * character), quoted when nested.
     */
    static void writeEscaped(TsvOutput out, byte[] s, boolean nested) {
        if (nested) {
            out.write('\'');
        }
        for (byte b : s) {
            switch (b) {
                case '\\':
                    out.write('\\').write('\\');
                    break;
                case '\'':
                    out.write('\\').write('\'');
                    break;
                case '\b':
                    out.write('\\').write('b');
                    break;
                case '\f':
                    out.write('\\').write('f');
                    break;
                case '\n':
                    out.write('\\').write('n');
                    break;
                case '\r':
                    out.write('\\').write('r');
                    break;
                case '\t':
                    out.write('\\').write('t');
                    break;
                case 0:
                    out.write('\\').write('0');
                    break;
                default:
                    out.write(b);
            }
        }
        if (nested) {
            out.write('\'');
        }
    }

    static void writeEscaped(TsvOutput out, String s, boolean nested) {
        writeEscaped(out, s.getBytes(StandardCharsets.UTF_8), nested);
    }

    /** Writes text that has no characters to escape (dates, UUIDs, addresses), quoted when nested. */
    private static void quotedIf(TsvOutput out, String s, boolean nested) {
        if (nested) {
            out.write('\'').write(s).write('\'');
        } else {
            out.write(s);
        }
    }

    private static int precision(ChType t, int arg) {
        String p = t.literalArg(arg);
        try {
            int n = Integer.parseInt(p);
            if (n >= 0 && n <= 9) {
                return n;
            }
        } catch (NumberFormatException | NullPointerException e) {
            // reported below
        }
        throw new UnsupportedTypeException(t.toString());
    }

    static String formatDecimal(BigDecimal d) {
        if (d.signum() == 0) {
            return "0";
        }
        return d.stripTrailingZeros().toPlainString();
    }

    private static String formatDate(ChType t, Object v) {
        LocalDate d;
        if (v instanceof LocalDate) {
            d = (LocalDate) v;
        } else if (v instanceof java.sql.Date) {
            d = ((java.sql.Date) v).toLocalDate();
        } else {
            throw new ValueMismatchException(t, v);
        }
        if (d.getYear() > MAX_TEXT_YEAR) {
            return "9999-12-31";
        }
        if (d.getYear() < 0) {
            return "0000-01-01";
        }
        return String.format("%04d-%02d-%02d", d.getYear(), d.getMonthValue(), d.getDayOfMonth());
    }

    /**
     * The last year that the server writes as text. The server writes a {@code Date32} or {@code DateTime64} value
     * after year 9999 as {@code 9999-12-31 [23:59:59]}, and a value before year 0 as {@code 0000-01-01 [00:00:00]}
     * (the wall-clock time in the time zone of the column; the fractional digits stay those of the value).
     */
    private static final int MAX_TEXT_YEAR = 9999;

    /** Writes the wall-clock time of the value, in the time zone that the client gave it. */
    private static String formatDateTime(ChType t, Object v, int precision) {
        LocalDateTime ldt;
        if (v instanceof ZonedDateTime) {
            ldt = ((ZonedDateTime) v).toLocalDateTime();
        } else if (v instanceof OffsetDateTime) {
            ldt = ((OffsetDateTime) v).toLocalDateTime();
        } else if (v instanceof LocalDateTime) {
            ldt = (LocalDateTime) v;
        } else if (v instanceof Timestamp) {
            ldt = ((Timestamp) v).toLocalDateTime();
        } else {
            throw new ValueMismatchException(t, v);
        }
        StringBuilder sb = new StringBuilder(32);
        if (ldt.getYear() > MAX_TEXT_YEAR) {
            sb.append("9999-12-31 23:59:59");
        } else if (ldt.getYear() < 0) {
            sb.append("0000-01-01 00:00:00");
        } else {
            sb.append(String.format("%04d-%02d-%02d %02d:%02d:%02d", ldt.getYear(), ldt.getMonthValue(),
                    ldt.getDayOfMonth(), ldt.getHour(), ldt.getMinute(), ldt.getSecond()));
        }
        if (precision > 0) {
            sb.append('.').append(String.format("%09d", ldt.getNano()), 0, precision);
        }
        return sb.toString();
    }

    private static final long MAX_TIME_SECONDS = 999L * 3600 + 59 * 60 + 59;

    /**
     * Writes a {@code Time} / {@code Time64} value: {@code [-]HH:MM:SS[.fff]}. The hours can be more than 24. The
     * client-v2 reader gives the value as a LocalDateTime on 1970-01-01 (the value is the time since its start).
     */
    private static String formatTime(ChType t, Object v, int precision) {
        long seconds;
        long nanos;
        if (v instanceof LocalDateTime) {
            LocalDateTime ldt = (LocalDateTime) v;
            seconds = ldt.toEpochSecond(ZoneOffset.UTC);
            nanos = ldt.getNano();
        } else if (v instanceof Duration) {
            seconds = ((Duration) v).getSeconds();
            nanos = ((Duration) v).getNano();
        } else if (v instanceof LocalTime) {
            seconds = ((LocalTime) v).toSecondOfDay();
            nanos = ((LocalTime) v).getNano();
        } else if (v instanceof java.sql.Time) {
            // jdbc-v2 gives Time and Time64 as java.sql.Time (a time of day) without the sign, the hours over 23
            // and the fraction (known failures in allowlist.txt). toLocalTime() drops the milliseconds that a
            // java.sql.Time can hold, so take them from getTime().
            java.sql.Time time = (java.sql.Time) v;
            seconds = time.toLocalTime().toSecondOfDay();
            nanos = Math.floorMod(time.getTime(), 1000L) * 1_000_000L;
        } else {
            throw new ValueMismatchException(t, v);
        }
        // (seconds, nanos) with 0 <= nanos < 1e9 is the value seconds + nanos / 1e9.
        boolean negative = seconds < 0;
        if (negative) {
            if (nanos > 0) {
                seconds = -seconds - 1;
                nanos = 1_000_000_000L - nanos;
            } else {
                seconds = -seconds;
            }
        }
        // The server writes a value above 999:59:59 as 999:59:59, with a zero fraction.
        if (seconds > MAX_TIME_SECONDS) {
            seconds = MAX_TIME_SECONDS;
            nanos = 0;
        }
        StringBuilder sb = new StringBuilder(24);
        if (negative) {
            sb.append('-');
        }
        sb.append(String.format("%02d:%02d:%02d", seconds / 3600, seconds / 60 % 60, seconds % 60));
        if (precision > 0) {
            sb.append('.').append(String.format("%09d", nanos), 0, precision);
        }
        return sb.toString();
    }

    private static String formatInterval(ChType t, Object v) {
        if (v instanceof Byte || v instanceof Short || v instanceof Integer || v instanceof Long
                || v instanceof BigInteger) {
            return v.toString();
        }
        if (v instanceof Period) {
            Period p = (Period) v;
            switch (t.base) {
                case "IntervalYear":
                    return String.valueOf(p.toTotalMonths() / 12);
                case "IntervalQuarter":
                    return String.valueOf(p.toTotalMonths() / 3);
                case "IntervalMonth":
                    return String.valueOf(p.toTotalMonths());
                case "IntervalWeek":
                    return String.valueOf(p.getDays() / 7);
                case "IntervalDay":
                    return String.valueOf(p.getDays());
                default:
                    break;
            }
        } else if (v instanceof Duration) {
            Duration d = (Duration) v;
            switch (t.base) {
                case "IntervalHour":
                    return String.valueOf(d.toHours());
                case "IntervalMinute":
                    return String.valueOf(d.toMinutes());
                case "IntervalSecond":
                    return String.valueOf(d.getSeconds());
                case "IntervalMillisecond":
                    return String.valueOf(d.toMillis());
                case "IntervalMicrosecond":
                    return String.valueOf(d.toNanos() / 1000);
                case "IntervalNanosecond":
                    return String.valueOf(d.toNanos());
                default:
                    break;
            }
        }
        throw new ValueMismatchException(t, v);
    }

    /**
     * Returns the elements of an array-like value: the client's ArrayValue, a List, a Java array or a
     * java.sql.Array. Returns null for any other value.
     */
    static List<?> sequence(Object v) {
        if (v instanceof BinaryStreamReader.ArrayValue) {
            return ((BinaryStreamReader.ArrayValue) v).asList();
        }
        if (v instanceof List) {
            return (List<?>) v;
        }
        if (v instanceof Object[]) {
            return Arrays.asList((Object[]) v);
        }
        if (v != null && v.getClass().isArray()) {
            int n = java.lang.reflect.Array.getLength(v);
            List<Object> out = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                out.add(java.lang.reflect.Array.get(v, i));
            }
            return out;
        }
        if (v instanceof java.sql.Array) {
            try {
                return sequence(((java.sql.Array) v).getArray());
            } catch (SQLException e) {
                throw new IllegalStateException("java.sql.Array.getArray() failed: " + e.getMessage(), e);
            }
        }
        return null;
    }

    private static void renderSequence(TsvOutput out, ChType inner, List<?> items, char open, char close) {
        out.write(open);
        for (int i = 0; i < items.size(); i++) {
            if (i > 0) {
                out.write(',');
            }
            renderValue(out, inner, items.get(i), true);
        }
        out.write(close);
    }

    private static void renderTuple(TsvOutput out, ChType t, Object v) {
        List<?> items = sequence(v);
        if (items == null || items.size() != t.args.size()) {
            throw new ValueMismatchException(t, v);
        }
        out.write('(');
        for (int i = 0; i < t.args.size(); i++) {
            if (i > 0) {
                out.write(',');
            }
            ChType.Arg a = t.args.get(i);
            if (a.type == null) {
                throw new UnsupportedTypeException(t.toString());
            }
            renderValue(out, a.type, items.get(i), true);
        }
        out.write(')');
    }

    /**
     * Writes a {@code Map} in the order of the Java map. The client gives a LinkedHashMap in server order; a map
     * without order cannot be written.
     */
    private static void renderMap(TsvOutput out, ChType t, Object v) {
        ChType kt = t.typeArg(0);
        ChType vt = t.typeArg(1);
        if (kt == null || vt == null || !(v instanceof Map)) {
            throw new ValueMismatchException(t, v);
        }
        Map<?, ?> m = (Map<?, ?>) v;
        if (m.size() > 1 && !(m instanceof java.util.LinkedHashMap) && !(m instanceof java.util.SortedMap)) {
            throw new UnsupportedTypeException(t + " (a " + m.getClass().getName() + " does not keep the server order)");
        }
        out.write('{');
        Iterator<? extends Map.Entry<?, ?>> it = m.entrySet().iterator();
        boolean first = true;
        while (it.hasNext()) {
            Map.Entry<?, ?> e = it.next();
            if (!first) {
                out.write(',');
            }
            first = false;
            renderValue(out, kt, e.getKey(), true);
            out.write(':');
            renderValue(out, vt, e.getValue(), true);
        }
        out.write('}');
    }

    private static void renderPoint(TsvOutput out, ChType t, Object v) {
        List<?> xy = sequence(v);
        if (xy == null || xy.size() != 2 || !(xy.get(0) instanceof Double) || !(xy.get(1) instanceof Double)) {
            throw new ValueMismatchException(t, v);
        }
        out.write('(').write(FloatFormat.formatDouble((Double) xy.get(0))).write(',')
                .write(FloatFormat.formatDouble((Double) xy.get(1))).write(')');
    }

    private static void renderGeo(TsvOutput out, ChType t, Object v, ChType element) {
        List<?> items = sequence(v);
        if (items == null) {
            throw new ValueMismatchException(t, v);
        }
        renderSequence(out, element, items, '[', ']');
    }

    /**
     * Writes a {@code Geometry} value: a Variant of {@code Point}, {@code Ring}, {@code LineString},
     * {@code MultiLineString}, {@code Polygon} and {@code MultiPolygon}. The client gives the value of the active
     * member as nested {@code double} arrays, so the number of array dimensions selects the text form: 1 is a Point,
     * 2 a Ring or LineString, 3 a Polygon or MultiLineString, 4 a MultiPolygon. The members with the same number of
     * dimensions have the same text.
     */
    private static void renderGeometry(TsvOutput out, ChType t, Object v) {
        int dimensions = 0;
        for (Class<?> c = v.getClass(); c.isArray(); c = c.getComponentType()) {
            dimensions++;
        }
        switch (dimensions) {
            case 1:
                renderPoint(out, ChType.of("Point"), v);
                return;
            case 2:
                renderGeo(out, ChType.of("LineString"), v, ChType.of("Point"));
                return;
            case 3:
                renderGeo(out, ChType.of("Polygon"), v, ChType.of("Ring"));
                return;
            case 4:
                renderGeo(out, ChType.of("MultiPolygon"), v, ChType.of("Polygon"));
                return;
            default:
                throw new ValueMismatchException(t, v);
        }
    }

    /**
     * Writes a {@code Variant} value. The client gives the value of the active member only, so the member type is
     * the first one that accepts the Java value.
     */
    private static void renderVariant(TsvOutput out, ChType t, Object v, boolean nested) {
        int mark = out.size();
        RuntimeException last = null;
        for (ChType.Arg a : t.args) {
            if (a.type == null) {
                throw new UnsupportedTypeException(t.toString());
            }
            try {
                renderValue(out, a.type, v, nested);
                return;
            } catch (ValueMismatchException | UnsupportedTypeException e) {
                out.truncate(mark);
                last = e;
            }
        }
        throw last != null ? last : new ValueMismatchException(t, v);
    }

    /**
     * Follows ClickHouse's {@code formatIPv6}: the longest run of two or more zero words becomes {@code ::}, and an
     * IPv4-mapped ({@code ::ffff:a.b.c.d}) or IPv4-compatible ({@code ::a.b.c.d}) address ends in dotted form.
     */
    static String formatIPv6(byte[] ip) {
        int[] words = new int[8];
        for (int i = 0; i < 8; i++) {
            words[i] = (ip[2 * i] & 0xff) << 8 | (ip[2 * i + 1] & 0xff);
        }
        int bestBase = -1;
        int bestLen = 0;
        for (int i = 0; i < 8;) {
            if (words[i] != 0) {
                i++;
                continue;
            }
            int j = i;
            while (j < 8 && words[j] == 0) {
                j++;
            }
            if (j - i > bestLen) {
                bestBase = i;
                bestLen = j - i;
            }
            i = j;
        }
        if (bestLen < 2) {
            bestBase = -1;
        }
        boolean ipv4Compatible = bestBase == 0 && bestLen == 6;
        boolean ipv4Mapped = bestBase == 0 && bestLen == 5 && words[5] == 0xffff;
        StringBuilder sb = new StringBuilder(40);
        for (int i = 0; i < 8; i++) {
            if (bestBase != -1 && i >= bestBase && i < bestBase + bestLen) {
                if (i == bestBase) {
                    sb.append(':');
                }
                continue;
            }
            if (i != 0) {
                sb.append(':');
            }
            if (i == 6 && (ipv4Compatible || ipv4Mapped)) {
                sb.append(ip[12] & 0xff).append('.').append(ip[13] & 0xff).append('.')
                        .append(ip[14] & 0xff).append('.').append(ip[15] & 0xff);
                return sb.toString();
            }
            sb.append(Integer.toHexString(words[i]));
        }
        if (bestBase != -1 && bestBase + bestLen == 8) {
            sb.append(':');
        }
        return sb.toString();
    }
}

package com.clickhouse.client.testdata;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.time.zone.ZoneOffsetTransition;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.LongFunction;

/**
 * Factory of {@link DataTypeGenerator}s for ClickHouse data types. Each generator produces boundary
 * values of the type, continuous blocks of values where the type has well known edges (dates, time)
 * and a fixed number of random values. Small integer types are generated exhaustively.
 */
public final class DataTypeGenerators {

    static final int RANDOM_COUNT = 50;
    static final int BLOCK_SIZE = 60;
    static final int NULL_FREQUENCY = 5;

    private static final int[] CHUNK_SIZES = {1, 2, 3, 5, 8, 13};
    private static final int[] POWER_OF_TWO_BOUNDARIES = {7, 8, 15, 16, 31, 32, 63, 64, 127, 128, 255};
    private static final long MAX_TIME_SECONDS = 999L * 3600 + 59 * 60 + 59;
    private static final long MAX_DATETIME_SECONDS = 0xFFFFFFFFL;
    private static final long NANOS_PER_SECOND = 1_000_000_000L;
    private static final char[] HEX = "0123456789ABCDEF".toCharArray();
    private static final List<String> INTERVAL_UNITS = Collections.unmodifiableList(Arrays.asList("Nanosecond",
            "Microsecond", "Millisecond", "Second", "Minute", "Hour", "Day", "Week", "Month", "Quarter", "Year"));

    private DataTypeGenerators() {
    }

    // Integers

    public static DataTypeGenerator int8() {
        return exhaustive("Int8", Byte.MIN_VALUE, Byte.MAX_VALUE, v -> (byte) v);
    }

    public static DataTypeGenerator uint8() {
        return exhaustive("UInt8", 0, 0xFF, v -> (short) v);
    }

    public static DataTypeGenerator int16() {
        return exhaustive("Int16", Short.MIN_VALUE, Short.MAX_VALUE, v -> (short) v);
    }

    public static DataTypeGenerator uint16() {
        return exhaustive("UInt16", 0, 0xFFFF, v -> (int) v);
    }

    public static DataTypeGenerator int32() {
        return bounded("Int32", signedMin(32), signedMax(32), BigInteger::intValue);
    }

    public static DataTypeGenerator uint32() {
        return bounded("UInt32", BigInteger.ZERO, unsignedMax(32), BigInteger::longValue);
    }

    public static DataTypeGenerator int64() {
        return bounded("Int64", signedMin(64), signedMax(64), BigInteger::longValue);
    }

    public static DataTypeGenerator uint64() {
        return bounded("UInt64", BigInteger.ZERO, unsignedMax(64), v -> v);
    }

    public static DataTypeGenerator int128() {
        return bounded("Int128", signedMin(128), signedMax(128), v -> v);
    }

    public static DataTypeGenerator uint128() {
        return bounded("UInt128", BigInteger.ZERO, unsignedMax(128), v -> v);
    }

    public static DataTypeGenerator int256() {
        return bounded("Int256", signedMin(256), signedMax(256), v -> v);
    }

    public static DataTypeGenerator uint256() {
        return bounded("UInt256", BigInteger.ZERO, unsignedMax(256), v -> v);
    }

    // Floats and decimals

    public static DataTypeGenerator float32() {
        return simple("Float32", random -> {
            List<Object> values = new ArrayList<>(Arrays.<Object>asList(0f, -0f, 1f, -1f, 0.1f, (float) Math.PI,
                    (float) Math.E, 16777215f, 16777216f, Float.MIN_VALUE, -Float.MIN_VALUE, Float.MIN_NORMAL,
                    Float.MAX_VALUE, -Float.MAX_VALUE, Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY));
            int size = values.size() + RANDOM_COUNT;
            while (values.size() < size) {
                float v = values.size() % 2 == 0 ? (random.nextFloat() - 0.5f) * 2000f
                        : Float.intBitsToFloat(random.nextInt());
                if (Float.isFinite(v)) {
                    values.add(v);
                }
            }
            return values;
        }, DataTypeGenerators::floatLiteral).plainText(DataTypeGenerators::floatText);
    }

    public static DataTypeGenerator float64() {
        return simple("Float64", random -> {
            List<Object> values = new ArrayList<>(Arrays.<Object>asList(0d, -0d, 1d, -1d, 0.1d, 0.1d + 0.2d, Math.PI,
                    Math.E, 9007199254740991d, 9007199254740992d, Double.MIN_VALUE, -Double.MIN_VALUE,
                    Double.MIN_NORMAL, Double.MAX_VALUE, -Double.MAX_VALUE, Double.NaN, Double.POSITIVE_INFINITY,
                    Double.NEGATIVE_INFINITY));
            int size = values.size() + RANDOM_COUNT;
            while (values.size() < size) {
                double v = values.size() % 2 == 0 ? (random.nextDouble() - 0.5d) * 2_000_000d
                        : Double.longBitsToDouble(random.nextLong());
                if (Double.isFinite(v)) {
                    values.add(v);
                }
            }
            return values;
        }, DataTypeGenerators::floatLiteral).plainText(DataTypeGenerators::floatText);
    }

    public static DataTypeGenerator decimal(int precision, int scale) {
        return decimal("Decimal(" + precision + ", " + scale + ")", precision, scale);
    }

    public static DataTypeGenerator decimal32(int scale) {
        return decimal("Decimal32(" + scale + ")", 9, scale);
    }

    public static DataTypeGenerator decimal64(int scale) {
        return decimal("Decimal64(" + scale + ")", 18, scale);
    }

    public static DataTypeGenerator decimal128(int scale) {
        return decimal("Decimal128(" + scale + ")", 38, scale);
    }

    public static DataTypeGenerator decimal256(int scale) {
        return decimal("Decimal256(" + scale + ")", 76, scale);
    }

    // Bool and strings

    public static DataTypeGenerator bool() {
        return simple("Bool", random -> Arrays.<Object>asList(true, false), Object::toString)
                .plainText(Object::toString);
    }

    /**
     * UTF-8 strings: special characters, escape sequences, all UTF-8 sequence lengths and a long string.
     */
    public static DataTypeGenerator string() {
        return simple("String", random -> {
            List<Object> values = new ArrayList<>(Arrays.<Object>asList("", "a", " ", " padded ", "'", "''", "\\",
                    "\\'", "\"", "`", "\t\n\r", "\0", "a\0b", "NULL", "null", "Привет, мир", "日本語テキスト",
                    "😀👍🏽", "é", "   ", "﻿",
                    repeat('x', 10_000)));
            for (int i = 0; i < RANDOM_COUNT; i++) {
                values.add(randomUtf8(random, random.nextInt(64)));
            }
            return values;
        }, v -> quote((String) v)).quotedText(v -> utf8((String) v));
    }

    /**
     * Strings with arbitrary bytes, including invalid UTF-8 sequences, represented as {@code byte[]}.
     */
    public static DataTypeGenerator binaryString() {
        return simple("String", random -> {
            byte[] allBytes = new byte[256];
            for (int i = 0; i < allBytes.length; i++) {
                allBytes[i] = (byte) i;
            }
            List<Object> values = new ArrayList<>(Arrays.<Object>asList(new byte[0], new byte[] {0}, allBytes,
                    new byte[] {(byte) 0xC3}, new byte[] {(byte) 0xFF, (byte) 0xFE},
                    new byte[] {(byte) 0xC0, (byte) 0xAF}, new byte[] {(byte) 0xED, (byte) 0xA0, (byte) 0x80}));
            for (int i = 0; i < RANDOM_COUNT; i++) {
                byte[] bytes = new byte[1 + random.nextInt(64)];
                random.nextBytes(bytes);
                values.add(bytes);
            }
            return values;
        }, v -> binaryLiteral((byte[]) v)).quotedText(v -> new String((byte[]) v, StandardCharsets.ISO_8859_1));
    }

    /**
     * Strings of exactly {@code length} UTF-8 bytes; shorter content is padded with {@code '\0'}.
     */
    public static DataTypeGenerator fixedString(int length) {
        if (length < 1) {
            throw new IllegalArgumentException("FixedString length must be positive: " + length);
        }
        return simple("FixedString(" + length + ")", random -> {
            Set<Object> values = new LinkedHashSet<>();
            values.add(padToBytes("", length));
            values.add(padToBytes("a", length));
            values.add(repeat('z', length));
            values.add(padToBytes(fillUtf8("ж", length), length));
            values.add(padToBytes(fillUtf8("😀", length), length));
            for (int i = 0; i < RANDOM_COUNT; i++) {
                values.add(randomFixedString(random, length));
            }
            return new ArrayList<>(values);
        }, v -> quote((String) v)).quotedText(v -> utf8((String) v));
    }

    // Dates and time

    public static DataTypeGenerator date() {
        return dates("Date", LocalDate.of(1970, 1, 1), LocalDate.of(2149, 6, 6),
                LocalDate.of(1972, 2, 1), LocalDate.of(2000, 2, 1), LocalDate.of(2038, 1, 1));
    }

    public static DataTypeGenerator date32() {
        return dates("Date32", LocalDate.of(1900, 1, 1), LocalDate.of(2299, 12, 31),
                LocalDate.of(1969, 12, 1), LocalDate.of(2000, 2, 1), LocalDate.of(2100, 2, 1));
    }

    public static DataTypeGenerator time() {
        return time("Time", 0);
    }

    public static DataTypeGenerator time64(int precision) {
        checkPrecision(precision);
        return time("Time64(" + precision + ")", precision);
    }

    /**
     * Values are written as unix timestamps so that they do not depend on the column timezone.
     */
    public static DataTypeGenerator dateTime(String timezone) {
        ZoneId zone = ZoneId.of(timezone);
        Instant max = Instant.ofEpochSecond(MAX_DATETIME_SECONDS);
        Duration second = Duration.ofSeconds(1);
        return simple("DateTime(" + quote(timezone) + ")", random -> {
            Set<Object> values = new LinkedHashSet<>();
            addInstants(values, zone, Instant.EPOCH, second, 0, BLOCK_SIZE - 1);
            addInstants(values, zone, Instant.ofEpochSecond(Integer.MAX_VALUE), second, -BLOCK_SIZE / 2, BLOCK_SIZE / 2);
            addInstants(values, zone, max, second, -BLOCK_SIZE + 1, 0);
            addTransitions(values, zone, second);
            for (int i = 0; i < RANDOM_COUNT; i++) {
                values.add(Instant.ofEpochSecond((long) (random.nextDouble() * MAX_DATETIME_SECONDS)).atZone(zone));
            }
            return new ArrayList<>(values);
        }, v -> Long.toString(((ZonedDateTime) v).toEpochSecond())).quotedText(v -> dateTimeText((ZonedDateTime) v, 0));
    }

    /**
     * Values are written as {@code toDateTime64(..., 'UTC')} so that they do not depend on the column timezone.
     */
    public static DataTypeGenerator dateTime64(int precision, String timezone) {
        checkPrecision(precision);
        ZoneId zone = ZoneId.of(timezone);
        Duration tick = Duration.ofNanos(pow10(9 - precision));
        Instant min = Instant.parse("1900-01-01T00:00:00Z");
        Instant max = precision == 9 ? Instant.ofEpochSecond(0, Long.MAX_VALUE)
                : Instant.parse("2299-12-31T23:59:59Z").plusNanos(NANOS_PER_SECOND - tick.toNanos());
        return simple("DateTime64(" + precision + ", " + quote(timezone) + ")", random -> {
            Set<Object> values = new LinkedHashSet<>();
            addInstants(values, zone, min, tick, 0, BLOCK_SIZE - 1);
            addInstants(values, zone, Instant.EPOCH, tick, -BLOCK_SIZE / 2, BLOCK_SIZE / 2);
            addInstants(values, zone, max, tick, -BLOCK_SIZE + 1, 0);
            addTransitions(values, zone, tick);
            long seconds = max.getEpochSecond() - min.getEpochSecond();
            for (int i = 0; i < RANDOM_COUNT; i++) {
                Instant instant = min.plusSeconds((long) (random.nextDouble() * seconds))
                        .plus(tick.multipliedBy(random.nextInt((int) (NANOS_PER_SECOND / tick.toNanos()))));
                values.add((instant.isAfter(max) ? max : instant).atZone(zone));
            }
            return new ArrayList<>(values);
        }, v -> dateTime64Literal((ZonedDateTime) v, precision))
                .quotedText(v -> dateTimeText((ZonedDateTime) v, precision));
    }

    // Network and identifiers

    public static DataTypeGenerator ipv4() {
        return simple("IPv4", random -> {
            Set<Object> values = new LinkedHashSet<>();
            for (String address : new String[] {"0.0.0.0", "127.0.0.1", "255.255.255.255", "10.0.0.1",
                    "172.16.0.1", "192.168.1.1", "169.254.0.1", "224.0.0.1", "8.8.8.8", "1.2.3.4"}) {
                values.add(inetAddress(address));
            }
            for (int i = 0; i < RANDOM_COUNT; i++) {
                byte[] bytes = new byte[4];
                random.nextBytes(bytes);
                values.add(inetAddress(bytes));
            }
            return new ArrayList<>(values);
        }, v -> quote(((InetAddress) v).getHostAddress())).quotedText(v -> ((InetAddress) v).getHostAddress());
    }

    /**
     * Includes IPv4-mapped addresses, which are written as {@code '::ffff:a.b.c.d'}.
     */
    public static DataTypeGenerator ipv6() {
        return simple("IPv6", random -> {
            Set<Object> values = new LinkedHashSet<>();
            for (String address : new String[] {"::", "::1", "::7f00:1", "2001:db8::1", "fe80::1", "ff02::1",
                    "2001:4860:4860::8888", "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"}) {
                values.add(inet6Address(inetAddress(address).getAddress()));
            }
            for (String address : new String[] {"0.0.0.0", "127.0.0.1", "255.255.255.255", "192.168.1.1"}) {
                byte[] bytes = new byte[16];
                bytes[10] = (byte) 0xFF;
                bytes[11] = (byte) 0xFF;
                System.arraycopy(inetAddress(address).getAddress(), 0, bytes, 12, 4);
                values.add(inet6Address(bytes));
            }
            for (int i = 0; i < RANDOM_COUNT; i++) {
                byte[] bytes = new byte[16];
                random.nextBytes(bytes);
                values.add(inet6Address(bytes));
            }
            return new ArrayList<>(values);
        }, v -> quote(ipv6String((Inet6Address) v))).quotedText(v -> ipv6Text((Inet6Address) v));
    }

    public static DataTypeGenerator uuid() {
        return simple("UUID", random -> {
            List<Object> values = new ArrayList<>(Arrays.<Object>asList(new UUID(0, 0), new UUID(0, 1),
                    new UUID(-1, -1), new UUID(Long.MIN_VALUE, Long.MAX_VALUE)));
            for (int i = 0; i < RANDOM_COUNT; i++) {
                values.add(new UUID(random.nextLong(), random.nextLong()));
            }
            return values;
        }, v -> quote(v.toString())).quotedText(Object::toString);
    }

    // Enums

    public static DataTypeGenerator enum8() {
        return enumeration("Enum8", defaultEnumConstants(Byte.MIN_VALUE, Byte.MAX_VALUE), false);
    }

    public static DataTypeGenerator enum16() {
        return enumeration("Enum16", defaultEnumConstants(Short.MIN_VALUE, Short.MAX_VALUE), false);
    }

    /**
     * @param type             {@code Enum8} or {@code Enum16}
     * @param constants        enum constant names and their numeric values
     * @param numericLiterals  write values as numbers instead of quoted names
     */
    public static DataTypeGenerator enumeration(String type, Map<String, Integer> constants, boolean numericLiterals) {
        if (constants.isEmpty()) {
            throw new IllegalArgumentException("Enum must have at least one constant");
        }
        StringJoiner definition = new StringJoiner(", ", type + "(", ")");
        for (Map.Entry<String, Integer> constant : constants.entrySet()) {
            definition.add(quote(constant.getKey()) + " = " + constant.getValue());
        }
        List<Object> names = new ArrayList<>(constants.keySet());
        return simple(definition.toString(), random -> names,
                v -> numericLiterals ? constants.get(v).toString() : quote((String) v))
                .quotedText(v -> utf8((String) v));
    }

    // Special types

    /**
     * @param unit one of {@code Nanosecond, Microsecond, Millisecond, Second, Minute, Hour, Day, Week,
     *             Month, Quarter, Year}
     */
    public static DataTypeGenerator interval(String unit) {
        if (!INTERVAL_UNITS.contains(unit)) {
            throw new IllegalArgumentException("Unknown interval unit: " + unit);
        }
        return simple("Interval" + unit, random -> {
            Set<Long> amounts = new LinkedHashSet<>(Arrays.asList(0L, 1L, -1L, 7L, 1000L));
            for (int i = 0; i < RANDOM_COUNT; i++) {
                amounts.add((long) (random.nextInt(2_000_000) - 1_000_000));
            }
            List<Object> values = new ArrayList<>();
            for (long amount : amounts) {
                values.add(intervalValue(unit, amount));
            }
            return values;
        }, v -> "toInterval" + unit + "(" + intervalAmount(unit, (TemporalAmount) v) + ")")
                .plainText(v -> Long.toString(intervalAmount(unit, (TemporalAmount) v))).notStorable();
    }

    public static List<DataTypeGenerator> intervals() {
        List<DataTypeGenerator> generators = new ArrayList<>();
        for (String unit : INTERVAL_UNITS) {
            generators.add(interval(unit));
        }
        return generators;
    }

    /**
     * {@code Nullable(Nothing)}, the type of a bare {@code NULL}; wrap with {@link #array} to get the type of {@code []}.
     */
    public static DataTypeGenerator nothing() {
        return simple("Nullable(Nothing)", random -> Collections.<Object>singletonList(null), v -> "NULL")
                .plainText(v -> "NULL")
                .notStorable();
    }

    // Geo

    public static DataTypeGenerator point() {
        return simple("Point", random -> {
            List<Object> values = new ArrayList<>(Arrays.<Object>asList(new double[] {0, 0},
                    new double[] {-180, -90}, new double[] {180, 90}, new double[] {-0.0, 0.0},
                    new double[] {Double.MAX_VALUE, -Double.MAX_VALUE},
                    new double[] {Double.MIN_VALUE, -Double.MIN_VALUE}));
            for (int i = 0; i < RANDOM_COUNT; i++) {
                values.add(new double[] {random.nextDouble() * 360 - 180, random.nextDouble() * 180 - 90});
            }
            return values;
        }, v -> {
            double[] point = (double[]) v;
            return "(" + floatLiteral(point[0]) + ", " + floatLiteral(point[1]) + ")";
        }).plainText(v -> {
            double[] point = (double[]) v;
            return "(" + floatText(point[0]) + "," + floatText(point[1]) + ")";
        });
    }

    public static DataTypeGenerator ring() {
        return geoArray("Ring", point(), double[].class);
    }

    public static DataTypeGenerator lineString() {
        return geoArray("LineString", point(), double[].class);
    }

    public static DataTypeGenerator polygon() {
        return geoArray("Polygon", ring(), double[][].class);
    }

    public static DataTypeGenerator multiLineString() {
        return geoArray("MultiLineString", lineString(), double[][].class);
    }

    public static DataTypeGenerator multiPolygon() {
        return geoArray("MultiPolygon", polygon(), double[][][].class);
    }

    // Composite types

    /**
     * Arrays covering all element values: an empty array followed by consecutive chunks of element values.
     */
    public static DataTypeGenerator array(DataTypeGenerator element) {
        return composite("Array(" + element.getType() + ")",
                random -> new ArrayList<Object>(chunks(element.generate(random), Integer.MAX_VALUE)),
                v -> join((List<?>) v, element, "[", "]"), element)
                .plainText(v -> joinText((List<?>) v, element, "[", "]"));
    }

    public static DataTypeGenerator tuple(DataTypeGenerator... elements) {
        StringJoiner type = new StringJoiner(", ", "Tuple(", ")");
        for (DataTypeGenerator element : elements) {
            type.add(element.getType());
        }
        return tuple(type.toString(), Arrays.asList(elements));
    }

    public static DataTypeGenerator namedTuple(Map<String, DataTypeGenerator> elements) {
        return tuple(namedType("Tuple", elements), new ArrayList<>(elements.values()));
    }

    /**
     * Maps covering all distinct keys and all values: an empty map followed by maps of consecutive entries.
     */
    public static DataTypeGenerator map(DataTypeGenerator key, DataTypeGenerator value) {
        return composite("Map(" + key.getType() + ", " + value.getType() + ")", random -> {
            List<Object> keys = new ArrayList<>(new LinkedHashSet<>(key.generate(random)));
            List<List<Object>> entries = zip(Arrays.asList(keys, value.generate(random)));
            List<Object> maps = new ArrayList<>();
            for (List<List<Object>> chunk : chunks(entries, keys.size())) {
                Map<Object, Object> map = new LinkedHashMap<>();
                for (List<Object> entry : chunk) {
                    map.put(entry.get(0), entry.get(1));
                }
                maps.add(map);
            }
            return maps;
        }, v -> {
            StringJoiner literal = new StringJoiner(", ", "{", "}");
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) v).entrySet()) {
                literal.add(key.toSqlLiteral(entry.getKey()) + ": " + value.toSqlLiteral(entry.getValue()));
            }
            return literal.toString();
        }, key, value).plainText(v -> {
            StringJoiner text = new StringJoiner(",", "{", "}");
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) v).entrySet()) {
                text.add(key.toQuotedText(entry.getKey()) + ":" + value.toQuotedText(entry.getValue()));
            }
            return text.toString();
        });
    }

    /**
     * Nested is written in its {@code Array(Tuple(...))} form, so it requires {@code flatten_nested = 0}.
     */
    public static DataTypeGenerator nested(Map<String, DataTypeGenerator> fields) {
        DataTypeGenerator rows = array(namedTuple(fields));
        return composite(namedType("Nested", fields), rows::generate, rows::toSqlLiteral, rows)
                .withText(rows::toTsv, rows::toQuotedText).withSetting("flatten_nested", "0");
    }

    /**
     * Inner values with {@code null} inserted before every {@value #NULL_FREQUENCY}-th value.
     */
    public static DataTypeGenerator nullable(DataTypeGenerator inner) {
        return composite("Nullable(" + inner.getType() + ")", random -> {
            List<Object> innerValues = inner.generate(random);
            List<Object> values = new ArrayList<>(innerValues.size() + innerValues.size() / NULL_FREQUENCY + 1);
            for (int i = 0; i < innerValues.size(); i++) {
                if (i % NULL_FREQUENCY == 0) {
                    values.add(null);
                }
                values.add(innerValues.get(i));
            }
            return values;
        }, inner::toSqlLiteral, inner).withText(inner::toTsv, inner::toQuotedText);
    }

    public static DataTypeGenerator lowCardinality(DataTypeGenerator inner) {
        return composite("LowCardinality(" + inner.getType() + ")", inner::generate, inner::toSqlLiteral, inner)
                .withText(inner::toTsv, inner::toQuotedText).withSetting("allow_suspicious_low_cardinality_types", "1");
    }

    /**
     * @return storable non-composite types with representative parameters
     */
    public static List<DataTypeGenerator> primitives() {
        return Arrays.asList(int8(), uint8(), int16(), uint16(), int32(), uint32(), int64(), uint64(), int128(),
                uint128(), int256(), uint256(), float32(), float64(), decimal(9, 2), decimal(18, 6),
                decimal(38, 0), decimal(76, 38), bool(), string(), binaryString(), fixedString(16), date(), date32(),
                time(), time64(3), time64(9), dateTime("UTC"), dateTime("America/New_York"),
                dateTime64(3, "UTC"), dateTime64(9, "Asia/Kolkata"), ipv4(), ipv6(), uuid(), enum8(), enum16(),
                point(), ring(), lineString(), polygon(), multiLineString(), multiPolygon());
    }

    // Shared helpers

    /**
     * Combines columns of values into rows; shorter columns repeat their values cyclically.
     */
    static List<List<Object>> zip(List<List<Object>> columns) {
        int size = 0;
        for (List<Object> column : columns) {
            if (column.isEmpty()) {
                throw new IllegalArgumentException("Cannot combine an empty list of values");
            }
            size = Math.max(size, column.size());
        }
        List<List<Object>> rows = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            List<Object> row = new ArrayList<>(columns.size());
            for (List<Object> column : columns) {
                row.add(column.get(i % column.size()));
            }
            rows.add(row);
        }
        return rows;
    }

    static String quoteIdentifier(String name) {
        return "`" + name.replace("\\", "\\\\").replace("`", "\\`") + "`";
    }

    static String quote(String value) {
        StringBuilder literal = new StringBuilder(value.length() + 2).append('\'');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '\'' || c == '\\') {
                literal.append('\\').append(c);
            } else if (c < 0x20 || c == 0x7F) {
                appendHexByte(literal, c);
            } else {
                literal.append(c);
            }
        }
        return literal.append('\'').toString();
    }

    /**
     * @return UTF-8 bytes of the value as a byte string, see {@link DataTypeGenerator#toTsv}
     */
    static String utf8(String value) {
        return new String(value.getBytes(StandardCharsets.UTF_8), StandardCharsets.ISO_8859_1);
    }

    /**
     * Escapes a byte string like ClickHouse does in {@code TabSeparated} and quoted values.
     */
    static String escape(String value) {
        StringBuilder escaped = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '\b':
                    escaped.append("\\b");
                    break;
                case '\f':
                    escaped.append("\\f");
                    break;
                case '\n':
                    escaped.append("\\n");
                    break;
                case '\r':
                    escaped.append("\\r");
                    break;
                case '\t':
                    escaped.append("\\t");
                    break;
                case '\0':
                    escaped.append("\\0");
                    break;
                case '\\':
                case '\'':
                    escaped.append('\\').append(c);
                    break;
                default:
                    escaped.append(c);
            }
        }
        return escaped.toString();
    }

    private static SimpleGenerator simple(String type, Function<Random, List<Object>> values,
            Function<Object, String> literal) {
        return new SimpleGenerator(type, values, literal, null, null, Collections.emptyMap(), true);
    }

    private static SimpleGenerator composite(String type, Function<Random, List<Object>> values,
            Function<Object, String> literal, DataTypeGenerator... children) {
        Map<String, String> settings = new LinkedHashMap<>();
        boolean storable = true;
        for (DataTypeGenerator child : children) {
            settings.putAll(child.getRequiredSettings());
            storable &= child.isStorable();
        }
        return new SimpleGenerator(type, values, literal, null, null, settings, storable);
    }

    private static DataTypeGenerator exhaustive(String type, long min, long max, LongFunction<Object> toJava) {
        return simple(type, random -> {
            List<Object> values = new ArrayList<>((int) (max - min + 1));
            for (long v = min; v <= max; v++) {
                values.add(toJava.apply(v));
            }
            return values;
        }, Object::toString).plainText(Object::toString);
    }

    private static DataTypeGenerator bounded(String type, BigInteger min, BigInteger max,
            Function<BigInteger, Object> toJava) {
        return simple(type, random -> {
            Set<BigInteger> values = new LinkedHashSet<>(Arrays.asList(min, min.add(BigInteger.ONE),
                    max.subtract(BigInteger.ONE), max, BigInteger.ZERO, BigInteger.ONE));
            if (min.signum() < 0) {
                values.add(BigInteger.ONE.negate());
            }
            for (int bits : POWER_OF_TWO_BOUNDARIES) {
                BigInteger power = BigInteger.ONE.shiftLeft(bits);
                for (BigInteger v : new BigInteger[] {power.subtract(BigInteger.ONE), power, power.negate(),
                        power.negate().subtract(BigInteger.ONE)}) {
                    if (v.compareTo(min) >= 0 && v.compareTo(max) <= 0) {
                        values.add(v);
                    }
                }
            }
            for (int i = 0; i < RANDOM_COUNT; i++) {
                values.add(randomBetween(random, min, max));
            }
            List<Object> result = new ArrayList<>(values.size());
            for (BigInteger v : values) {
                result.add(toJava.apply(v));
            }
            return result;
        }, Object::toString).plainText(Object::toString);
    }

    private static DataTypeGenerator decimal(String type, int precision, int scale) {
        if (precision < 1 || precision > 76 || scale < 0 || scale > precision) {
            throw new IllegalArgumentException("Invalid decimal precision " + precision + " and scale " + scale);
        }
        return simple(type, random -> {
            BigInteger maxUnscaled = BigInteger.TEN.pow(precision).subtract(BigInteger.ONE);
            Set<Object> values = new LinkedHashSet<>(Arrays.asList(new BigDecimal(BigInteger.ZERO, scale),
                    new BigDecimal(maxUnscaled, scale), new BigDecimal(maxUnscaled.negate(), scale),
                    new BigDecimal(BigInteger.ONE, scale), new BigDecimal(BigInteger.ONE.negate(), scale)));
            if (precision > scale) {
                values.add(BigDecimal.ONE.setScale(scale));
                values.add(BigDecimal.ONE.negate().setScale(scale));
            }
            for (int i = 0; i < RANDOM_COUNT; i++) {
                BigInteger digits = BigInteger.TEN.pow(1 + random.nextInt(precision)).subtract(BigInteger.ONE);
                BigInteger unscaled = randomBetween(random, BigInteger.ZERO, digits);
                values.add(new BigDecimal(random.nextBoolean() ? unscaled : unscaled.negate(), scale));
            }
            return new ArrayList<>(values);
        }, v -> ((BigDecimal) v).toPlainString()).plainText(v -> decimalText((BigDecimal) v));
    }

    private static DataTypeGenerator dates(String type, LocalDate min, LocalDate max, LocalDate... blockStarts) {
        return simple(type, random -> {
            Set<Object> values = new LinkedHashSet<>();
            addDays(values, min);
            for (LocalDate start : blockStarts) {
                addDays(values, start);
            }
            addDays(values, max.minusDays(BLOCK_SIZE - 1));
            long days = ChronoUnit.DAYS.between(min, max);
            for (int i = 0; i < RANDOM_COUNT; i++) {
                values.add(min.plusDays((long) (random.nextDouble() * (days + 1))));
            }
            return new ArrayList<>(values);
        }, v -> "'" + v + "'").quotedText(Object::toString);
    }

    private static DataTypeGenerator time(String type, int precision) {
        long step = pow10(9 - precision);
        Duration max = Duration.ofSeconds(MAX_TIME_SECONDS, NANOS_PER_SECOND - step);
        Duration day = Duration.ofDays(1);
        return simple(type, random -> {
            Set<Object> values = new LinkedHashSet<>(Arrays.asList(Duration.ZERO, max, max.negated(), day,
                    day.negated(), day.minusSeconds(1)));
            addTicks(values, Duration.ZERO, step, -BLOCK_SIZE / 2, BLOCK_SIZE / 2);
            addTicks(values, day, step, -BLOCK_SIZE / 2, BLOCK_SIZE / 2);
            addTicks(values, max, step, -BLOCK_SIZE + 1, 0);
            addTicks(values, max.negated(), step, 0, BLOCK_SIZE - 1);
            for (int i = 0; i < RANDOM_COUNT; i++) {
                Duration value = Duration.ofSeconds(random.nextInt((int) MAX_TIME_SECONDS + 1),
                        random.nextInt((int) (NANOS_PER_SECOND / step)) * step);
                values.add(random.nextBoolean() ? value : value.negated());
            }
            return new ArrayList<>(values);
        }, v -> "'" + timeText((Duration) v, precision) + "'")
                .quotedText(v -> timeText((Duration) v, precision)).withSetting("enable_time_time64_type", "1");
    }

    private static DataTypeGenerator tuple(String type, List<DataTypeGenerator> elements) {
        if (elements.isEmpty()) {
            throw new IllegalArgumentException("Tuple must have at least one element");
        }
        return composite(type, random -> {
            List<List<Object>> columns = new ArrayList<>(elements.size());
            for (DataTypeGenerator element : elements) {
                columns.add(element.generate(random));
            }
            return new ArrayList<Object>(zip(columns));
        }, v -> {
            List<?> tuple = (List<?>) v;
            StringJoiner literal = new StringJoiner(", ", tuple.size() == 1 ? "tuple(" : "(", ")");
            for (int i = 0; i < tuple.size(); i++) {
                literal.add(elements.get(i).toSqlLiteral(tuple.get(i)));
            }
            return literal.toString();
        }, elements.toArray(new DataTypeGenerator[0])).plainText(v -> {
            List<?> tuple = (List<?>) v;
            StringJoiner text = new StringJoiner(",", "(", ")");
            for (int i = 0; i < tuple.size(); i++) {
                text.add(elements.get(i).toQuotedText(tuple.get(i)));
            }
            return text.toString();
        });
    }

    private static DataTypeGenerator geoArray(String type, DataTypeGenerator element, Class<?> elementClass) {
        return simple(type, random -> {
            List<Object> values = new ArrayList<>();
            for (List<Object> chunk : chunks(element.generate(random), Integer.MAX_VALUE)) {
                Object array = Array.newInstance(elementClass, chunk.size());
                for (int i = 0; i < chunk.size(); i++) {
                    Array.set(array, i, chunk.get(i));
                }
                values.add(array);
            }
            return values;
        }, v -> {
            StringJoiner literal = new StringJoiner(", ", "[", "]");
            for (int i = 0; i < Array.getLength(v); i++) {
                literal.add(element.toSqlLiteral(Array.get(v, i)));
            }
            return literal.toString();
        }).plainText(v -> {
            StringJoiner text = new StringJoiner(",", "[", "]");
            for (int i = 0; i < Array.getLength(v); i++) {
                text.add(element.toQuotedText(Array.get(v, i)));
            }
            return text.toString();
        });
    }

    private static String namedType(String base, Map<String, DataTypeGenerator> fields) {
        if (fields.isEmpty()) {
            throw new IllegalArgumentException(base + " must have at least one field");
        }
        StringJoiner type = new StringJoiner(", ", base + "(", ")");
        for (Map.Entry<String, DataTypeGenerator> field : fields.entrySet()) {
            type.add(quoteIdentifier(field.getKey()) + " " + field.getValue().getType());
        }
        return type.toString();
    }

    private static <T> List<List<T>> chunks(List<T> values, int maxSize) {
        List<List<T>> chunks = new ArrayList<>();
        chunks.add(new ArrayList<>());
        int offset = 0;
        for (int i = 0; offset < values.size(); i++) {
            int size = Math.min(Math.min(CHUNK_SIZES[i % CHUNK_SIZES.length], maxSize), values.size() - offset);
            chunks.add(new ArrayList<>(values.subList(offset, offset + size)));
            offset += size;
        }
        return chunks;
    }

    private static String join(List<?> values, DataTypeGenerator element, String prefix, String suffix) {
        StringJoiner literal = new StringJoiner(", ", prefix, suffix);
        for (Object value : values) {
            literal.add(element.toSqlLiteral(value));
        }
        return literal.toString();
    }

    private static String joinText(List<?> values, DataTypeGenerator element, String prefix, String suffix) {
        StringJoiner text = new StringJoiner(",", prefix, suffix);
        for (Object value : values) {
            text.add(element.toQuotedText(value));
        }
        return text.toString();
    }

    private static BigInteger signedMin(int bits) {
        return BigInteger.ONE.shiftLeft(bits - 1).negate();
    }

    private static BigInteger signedMax(int bits) {
        return BigInteger.ONE.shiftLeft(bits - 1).subtract(BigInteger.ONE);
    }

    private static BigInteger unsignedMax(int bits) {
        return BigInteger.ONE.shiftLeft(bits).subtract(BigInteger.ONE);
    }

    private static BigInteger randomBetween(Random random, BigInteger min, BigInteger max) {
        BigInteger range = max.subtract(min);
        BigInteger value;
        do {
            value = new BigInteger(range.bitLength(), random);
        } while (value.compareTo(range) > 0);
        return min.add(value);
    }

    private static long pow10(int exponent) {
        long value = 1;
        for (int i = 0; i < exponent; i++) {
            value *= 10;
        }
        return value;
    }

    private static void checkPrecision(int precision) {
        if (precision < 0 || precision > 9) {
            throw new IllegalArgumentException("Precision must be in range [0, 9]: " + precision);
        }
    }

    private static String floatLiteral(Object value) {
        double v = ((Number) value).doubleValue();
        if (Double.isNaN(v)) {
            return "nan";
        } else if (Double.isInfinite(v)) {
            return v > 0 ? "inf" : "-inf";
        }
        return value.toString();
    }

    /**
     * Shortest decimal representation that parses back to the same value, formatted like ClickHouse does:
     * positional notation for decimal exponents from -5 to 20, scientific notation otherwise.
     */
    private static String floatText(Object value) {
        double v = ((Number) value).doubleValue();
        if (Double.isNaN(v) || Double.isInfinite(v)) {
            return floatLiteral(value);
        } else if (v == 0) {
            return 1 / v < 0 ? "-0" : "0";
        }
        boolean isFloat = value instanceof Float;
        BigDecimal exact = new BigDecimal(v);
        BigDecimal shortest = exact;
        for (int digits = 1; digits <= 17; digits++) {
            shortest = exact.round(new MathContext(digits, RoundingMode.HALF_EVEN));
            if (isFloat ? shortest.floatValue() == (float) v : shortest.doubleValue() == v) {
                break;
            }
        }
        shortest = shortest.stripTrailingZeros();
        String digits = shortest.unscaledValue().abs().toString();
        // value = 0.digits * 10^point
        int point = digits.length() - shortest.scale();
        StringBuilder text = new StringBuilder(v < 0 ? "-" : "");
        if (point > 21 || point < -5) {
            text.append(digits.charAt(0));
            if (digits.length() > 1) {
                text.append('.').append(digits, 1, digits.length());
            }
            text.append('e').append(point - 1);
        } else if (point <= 0) {
            text.append("0.").append(repeat('0', -point)).append(digits);
        } else if (point >= digits.length()) {
            text.append(digits).append(repeat('0', point - digits.length()));
        } else {
            text.append(digits, 0, point).append('.').append(digits, point, digits.length());
        }
        return text.toString();
    }

    /**
     * Decimals are written without trailing zeros ({@code output_format_decimal_trailing_zeros = 0}).
     */
    private static String decimalText(BigDecimal value) {
        return value.signum() == 0 ? "0" : value.stripTrailingZeros().toPlainString();
    }

    private static String binaryLiteral(byte[] value) {
        StringBuilder literal = new StringBuilder(value.length * 4 + 2).append('\'');
        for (byte b : value) {
            appendHexByte(literal, b & 0xFF);
        }
        return literal.append('\'').toString();
    }

    private static void appendHexByte(StringBuilder sb, int b) {
        sb.append("\\x").append(HEX[b >> 4]).append(HEX[b & 0xF]);
    }

    private static String repeat(char c, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, c);
        return new String(chars);
    }

    private static int utf8Length(String value) {
        return value.getBytes(StandardCharsets.UTF_8).length;
    }

    private static String padToBytes(String value, int length) {
        return value + repeat('\0', length - utf8Length(value));
    }

    private static String fillUtf8(String unit, int length) {
        StringBuilder value = new StringBuilder();
        int unitLength = utf8Length(unit);
        for (int bytes = unitLength; bytes <= length; bytes += unitLength) {
            value.append(unit);
        }
        return value.toString();
    }

    /**
     * Random code points covering all UTF-8 sequence lengths (1 to 4 bytes).
     */
    private static String randomUtf8(Random random, int length) {
        StringBuilder value = new StringBuilder(length * 2);
        for (int i = 0; i < length; i++) {
            int codePoint;
            switch (random.nextInt(4)) {
                case 0:
                    codePoint = 0x20 + random.nextInt(0x7F - 0x20);
                    break;
                case 1:
                    codePoint = 0x80 + random.nextInt(0x800 - 0x80);
                    break;
                case 2:
                    do {
                        codePoint = 0x800 + random.nextInt(0x10000 - 0x800);
                    } while (codePoint >= Character.MIN_SURROGATE && codePoint <= Character.MAX_SURROGATE);
                    break;
                default:
                    codePoint = 0x10000 + random.nextInt(Character.MAX_CODE_POINT + 1 - 0x10000);
            }
            value.appendCodePoint(codePoint);
        }
        return value.toString();
    }

    private static String randomFixedString(Random random, int length) {
        StringBuilder value = new StringBuilder();
        int bytes = 0;
        while (bytes < length) {
            String c = randomUtf8(random, 1);
            if (bytes + utf8Length(c) > length) {
                c = "a";
            }
            value.append(c);
            bytes += utf8Length(c);
        }
        return value.toString();
    }

    private static void addDays(Set<Object> values, LocalDate start) {
        for (int i = 0; i < BLOCK_SIZE; i++) {
            values.add(start.plusDays(i));
        }
    }

    private static void addTicks(Set<Object> values, Duration base, long step, int from, int to) {
        for (int i = from; i <= to; i++) {
            values.add(base.plusNanos(i * step));
        }
    }

    private static void addInstants(Set<Object> values, ZoneId zone, Instant base, Duration step, int from, int to) {
        for (int i = from; i <= to; i++) {
            values.add(base.plus(step.multipliedBy(i)).atZone(zone));
        }
    }

    /**
     * Adds values around the first two offset transitions (usually DST start and end) of 2021.
     */
    private static void addTransitions(Set<Object> values, ZoneId zone, Duration step) {
        Instant from = Instant.parse("2021-01-01T00:00:00Z");
        for (int i = 0; i < 2; i++) {
            ZoneOffsetTransition transition = zone.getRules().nextTransition(from);
            if (transition == null) {
                return;
            }
            addInstants(values, zone, transition.getInstant(), step, -BLOCK_SIZE / 2, BLOCK_SIZE / 2);
            addInstants(values, zone, transition.getInstant(), Duration.ofMinutes(30), -6, 6);
            from = transition.getInstant();
        }
    }

    private static String timeText(Duration value, int precision) {
        Duration abs = value.abs();
        long seconds = abs.getSeconds();
        StringBuilder text = new StringBuilder();
        if (value.isNegative()) {
            text.append('-');
        }
        appendPadded(text, seconds / 3600, 2).append(':');
        appendPadded(text, seconds / 60 % 60, 2).append(':');
        appendPadded(text, seconds % 60, 2);
        appendFraction(text, abs.getNano(), precision);
        return text.toString();
    }

    private static String dateTimeText(ZonedDateTime value, int precision) {
        StringBuilder text = new StringBuilder().append(value.toLocalDate()).append(' ');
        appendPadded(text, value.getHour(), 2).append(':');
        appendPadded(text, value.getMinute(), 2).append(':');
        appendPadded(text, value.getSecond(), 2);
        appendFraction(text, value.getNano(), precision);
        return text.toString();
    }

    private static String dateTime64Literal(ZonedDateTime value, int precision) {
        ZonedDateTime utc = value.withZoneSameInstant(ZoneOffset.UTC);
        StringBuilder literal = new StringBuilder("toDateTime64('").append(utc.toLocalDate()).append(' ');
        appendPadded(literal, utc.getHour(), 2).append(':');
        appendPadded(literal, utc.getMinute(), 2).append(':');
        appendPadded(literal, utc.getSecond(), 2);
        appendFraction(literal, utc.getNano(), precision);
        return literal.append("', ").append(precision).append(", 'UTC')").toString();
    }

    private static StringBuilder appendPadded(StringBuilder sb, long value, int width) {
        String digits = Long.toString(value);
        for (int i = digits.length(); i < width; i++) {
            sb.append('0');
        }
        return sb.append(digits);
    }

    private static void appendFraction(StringBuilder sb, int nanos, int precision) {
        if (precision > 0) {
            String digits = Long.toString(NANOS_PER_SECOND + nanos).substring(1);
            sb.append('.').append(digits, 0, precision);
        }
    }

    private static InetAddress inetAddress(String address) {
        try {
            return InetAddress.getByName(address);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException(address, e);
        }
    }

    private static InetAddress inetAddress(byte[] address) {
        try {
            return InetAddress.getByAddress(address);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException(Arrays.toString(address), e);
        }
    }

    private static Inet6Address inet6Address(byte[] address) {
        try {
            return Inet6Address.getByAddress(null, address, -1);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException(Arrays.toString(address), e);
        }
    }

    /**
     * IPv6 address as ClickHouse writes it: the longest run of two or more zero groups is compressed and
     * IPv4-mapped ({@code ::ffff:a.b.c.d}) and IPv4-compatible ({@code ::a.b.c.d}) addresses end with IPv4.
     */
    private static String ipv6Text(Inet6Address address) {
        byte[] bytes = address.getAddress();
        int[] groups = new int[8];
        for (int i = 0; i < groups.length; i++) {
            groups[i] = (bytes[2 * i] & 0xFF) << 8 | bytes[2 * i + 1] & 0xFF;
        }
        int bestStart = -1;
        int bestLength = 0;
        for (int i = 0; i < groups.length; i++) {
            int length = 0;
            while (i + length < groups.length && groups[i + length] == 0) {
                length++;
            }
            if (length > bestLength) {
                bestStart = i;
                bestLength = length;
            }
            i += length;
        }
        if (bestStart == 0 && (bestLength == 6 || bestLength == 5 && groups[5] == 0xFFFF)) {
            return (bestLength == 6 ? "::" : "::ffff:") + (bytes[12] & 0xFF) + "." + (bytes[13] & 0xFF) + "."
                    + (bytes[14] & 0xFF) + "." + (bytes[15] & 0xFF);
        }
        if (bestLength < 2) {
            bestStart = -1;
        }
        StringBuilder text = new StringBuilder();
        for (int i = 0; i < groups.length; i++) {
            if (i == bestStart) {
                text.append("::");
                i += bestLength - 1;
                continue;
            }
            if (text.length() > 0 && text.charAt(text.length() - 1) != ':') {
                text.append(':');
            }
            text.append(Integer.toHexString(groups[i]));
        }
        return text.toString();
    }

    private static String ipv6String(Inet6Address address) {
        byte[] bytes = address.getAddress();
        for (int i = 0; i < 10; i++) {
            if (bytes[i] != 0) {
                return address.getHostAddress();
            }
        }
        if (bytes[10] == (byte) 0xFF && bytes[11] == (byte) 0xFF) {
            return "::ffff:" + (bytes[12] & 0xFF) + "." + (bytes[13] & 0xFF) + "." + (bytes[14] & 0xFF) + "."
                    + (bytes[15] & 0xFF);
        }
        return address.getHostAddress();
    }

    private static Map<String, Integer> defaultEnumConstants(int min, int max) {
        Map<String, Integer> constants = new LinkedHashMap<>();
        constants.put("min", min);
        constants.put("minus one", -1);
        constants.put("zero", 0);
        constants.put("quote'", 1);
        constants.put("back\\slash", 2);
        constants.put("юникод", 3);
        constants.put("😀", 4);
        constants.put("max", max);
        return constants;
    }

    private static TemporalAmount intervalValue(String unit, long amount) {
        switch (unit) {
            case "Year":
                return Period.ofYears((int) amount);
            case "Quarter":
                return Period.ofMonths(3 * (int) amount);
            case "Month":
                return Period.ofMonths((int) amount);
            case "Week":
                return Period.ofWeeks((int) amount);
            case "Day":
                return Period.ofDays((int) amount);
            case "Hour":
                return Duration.ofHours(amount);
            case "Minute":
                return Duration.ofMinutes(amount);
            case "Second":
                return Duration.ofSeconds(amount);
            case "Millisecond":
                return Duration.ofMillis(amount);
            case "Microsecond":
                return Duration.ofNanos(amount * 1000);
            default:
                return Duration.ofNanos(amount);
        }
    }

    private static long intervalAmount(String unit, TemporalAmount value) {
        switch (unit) {
            case "Year":
                return ((Period) value).getYears();
            case "Quarter":
                return ((Period) value).toTotalMonths() / 3;
            case "Month":
                return ((Period) value).toTotalMonths();
            case "Week":
                return ((Period) value).getDays() / 7;
            case "Day":
                return ((Period) value).getDays();
            case "Hour":
                return ((Duration) value).toHours();
            case "Minute":
                return ((Duration) value).toMinutes();
            case "Second":
                return ((Duration) value).getSeconds();
            case "Millisecond":
                return ((Duration) value).toMillis();
            case "Microsecond":
                return ((Duration) value).toNanos() / 1000;
            default:
                return ((Duration) value).toNanos();
        }
    }

    private static final class SimpleGenerator implements DataTypeGenerator {
        private final String type;
        private final Function<Random, List<Object>> values;
        private final Function<Object, String> literal;
        private final Function<Object, String> tsv;
        private final Function<Object, String> quotedText;
        private final Map<String, String> settings;
        private final boolean storable;

        SimpleGenerator(String type, Function<Random, List<Object>> values, Function<Object, String> literal,
                Function<Object, String> tsv, Function<Object, String> quotedText, Map<String, String> settings,
                boolean storable) {
            this.type = type;
            this.values = values;
            this.literal = literal;
            this.tsv = tsv;
            this.quotedText = quotedText;
            this.settings = Collections.unmodifiableMap(new LinkedHashMap<>(settings));
            this.storable = storable;
        }

        SimpleGenerator withSetting(String name, String value) {
            Map<String, String> newSettings = new LinkedHashMap<>(settings);
            newSettings.put(name, value);
            return new SimpleGenerator(type, values, literal, tsv, quotedText, newSettings, storable);
        }

        SimpleGenerator notStorable() {
            return new SimpleGenerator(type, values, literal, tsv, quotedText, settings, false);
        }

        SimpleGenerator withText(Function<Object, String> tsv, Function<Object, String> quotedText) {
            return new SimpleGenerator(type, values, literal, tsv, quotedText, settings, storable);
        }

        /**
         * Text written as is, both in a column and inside composite types (numbers, composite types).
         */
        SimpleGenerator plainText(Function<Object, String> text) {
            return withText(text, text);
        }

        /**
         * Text escaped in a column and quoted inside composite types (strings, dates, identifiers).
         */
        SimpleGenerator quotedText(Function<Object, String> text) {
            return withText(v -> escape(text.apply(v)), v -> "'" + escape(text.apply(v)) + "'");
        }

        @Override
        public String getType() {
            return type;
        }

        @Override
        public List<Object> generate(Random random) {
            return values.apply(random);
        }

        @Override
        public String toSqlLiteral(Object value) {
            return value == null ? "NULL" : literal.apply(value);
        }

        @Override
        public String toTsv(Object value) {
            return value == null ? "\\N" : tsv.apply(value);
        }

        @Override
        public String toQuotedText(Object value) {
            return value == null ? "NULL" : quotedText.apply(value);
        }

        @Override
        public Map<String, String> getRequiredSettings() {
            return settings;
        }

        @Override
        public boolean isStorable() {
            return storable;
        }

        @Override
        public String toString() {
            return type;
        }
    }
}

package com.clickhouse.client.cli;

import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class TsvRendererTest {

    @DataProvider
    public Object[][] doubles() {
        double a = 0.1;
        double b = 0.2;
        // The expected text is what ClickHouse 26.10 writes for FORMAT TSV.
        return new Object[][] {
                {1e15, "1000000000000000"},
                {1e20, "100000000000000000000"},
                {1e21, "1e21"},
                {1.2345e21, "1.2345e21"},
                {1.2345e20, "123450000000000000000"},
                {0.0001, "0.0001"},
                {1e-6, "0.000001"},
                {1.5e-6, "0.0000015"},
                {1e-7, "1e-7"},
                {1.5e-7, "1.5e-7"},
                {1.5e300, "1.5e300"},
                {a + b, "0.30000000000000004"},
                {5e-324, "5e-324"},
                {-12345678.9, "-12345678.9"},
                {100.0, "100"},
                {Double.NaN, "nan"},
                {Double.POSITIVE_INFINITY, "inf"},
                {Double.NEGATIVE_INFINITY, "-inf"},
                {-0.0, "-0"},
                {0.0, "0"},
                // Double.toString of JDK 17 gives 2.82879384806159008E17 (not the shortest text).
                {2.82879384806159E17, "282879384806159000"},
                {1.0E23, "1e23"},
                {2.0E-3, "0.002"},
        };
    }

    @Test(dataProvider = "doubles")
    public void testFormatDouble(double in, String expected) {
        assertEquals(FloatFormat.formatDouble(in), expected);
    }

    @DataProvider
    public Object[][] floats() {
        return new Object[][] {
                {0.1f, "0.1"},
                {3.4028235e38f, "3.4028235e38"},
                {16777216f, "16777216"},
                {1.5f, "1.5"},
                {-0.0f, "-0"},
                {Float.NaN, "nan"},
                {1e-7f, "1e-7"},
        };
    }

    @Test(dataProvider = "floats")
    public void testFormatFloat(float in, String expected) {
        assertEquals(FloatFormat.formatFloat(in), expected);
    }

    @DataProvider
    public Object[][] ipv6() {
        return new Object[][] {
                {"::ffff:1.2.3.4", "::ffff:1.2.3.4"},
                {"::1.2.3.4", "::1.2.3.4"},
                {"::", "::"},
                {"::1", "::1"},
                {"2001:db8::1", "2001:db8::1"},
                {"1::", "1::"},
                {"2001:db8:0:1::1", "2001:db8:0:1::1"},
                {"1:0:0:2:0:0:0:3", "1:0:0:2::3"},
                {"1:2:3:4:5:6:7:8", "1:2:3:4:5:6:7:8"},
        };
    }

    @Test(dataProvider = "ipv6")
    public void testFormatIPv6(String in, String expected) throws Exception {
        byte[] b = InetAddress.getByName(in).getAddress();
        if (b.length == 4) {
            // An IPv4-mapped address: InetAddress gives the IPv4 address.
            assertEquals(render("IPv6", InetAddress.getByName(in)), expected);
            return;
        }
        assertEquals(TsvRenderer.formatIPv6(b), expected);
    }

    @DataProvider
    public Object[][] typeNames() {
        return new Object[][] {
                {"UInt8"},
                {"Nullable(String)"},
                {"Array(Nullable(DateTime64(3, 'UTC')))"},
                {"Map(String, Array(Tuple(UInt8, String)))"},
                {"Tuple(a Int32, b String)"},
                {"Enum8('a' = 1, 'b,c' = 2)"},
                {"Decimal(9, 2)"},
                {"LowCardinality(Nullable(String))"},
                {"SimpleAggregateFunction(sum, UInt64)"},
                {"AggregateFunction(quantiles(0.5, 0.9), UInt64)"},
        };
    }

    @Test(dataProvider = "typeNames")
    public void testParseType(String name) {
        assertEquals(ChType.parse(name).toString(), name);
    }

    @Test
    public void testParseSimpleAggregateFunctionType() {
        // The last argument is the value type, also for the types that the renderer does not write.
        ChType t = ChType.parse("SimpleAggregateFunction(anyLast, JSON)");
        assertEquals(t.typeArg(1).base, "JSON");
        assertEquals(t.args.get(0).literal, "anyLast");
    }

    @Test
    public void testParseNamedTuple() {
        ChType t = ChType.parse("Tuple(`a b` Int32, c Array(String))");
        assertEquals(t.args.size(), 2);
        assertEquals(t.args.get(0).name, "a b");
        assertEquals(t.args.get(1).name, "c");
        assertEquals(t.args.get(1).type.base, "Array");
        assertThrows(IllegalArgumentException.class, () -> ChType.parse("Array(String"));
    }

    private static String render(String type, Object value) {
        TsvOutput out = new TsvOutput();
        new TsvRenderer(type).render(out, value, false);
        return out.toString();
    }

    @DataProvider
    public Object[][] values() throws Exception {
        ZonedDateTime utc = ZonedDateTime.of(2020, 1, 1, 0, 0, 0, 120_000_000, ZoneOffset.UTC);
        Map<String, Short> map = new LinkedHashMap<>();
        map.put("k", (short) 1);
        map.put("a", (short) 2);
        return new Object[][] {
                {"String", "a\tb\\c\nd'e\0\b\f\u0001", "a\\tb\\\\c\\nd\\'e\\0\\b\\f\u0001"},
                {"Nullable(String)", null, "\\N"},
                {"Nullable(String)", "x", "x"},
                {"Array(Nullable(String))", Arrays.asList("a'b", null), "['a\\'b',NULL]"},
                {"Tuple(UInt8, String, Nullable(UInt8))", new Object[] {(short) 1, "x", null}, "(1,'x',NULL)"},
                {"Tuple(a UInt8, b String)", new Object[] {(short) 1, "x"}, "(1,'x')"},
                {"Map(String, UInt8)", map, "{'k':1,'a':2}"},
                {"Int128", BigInteger.valueOf(-5), "-5"},
                {"UInt64", new BigInteger("18446744073709551615"), "18446744073709551615"},
                {"Decimal(9, 2)", new BigDecimal("1.50"), "1.5"},
                {"Decimal(9, 2)", new BigDecimal("0.00"), "0"},
                {"Decimal(38, 3)", new BigDecimal("100.000"), "100"},
                {"DateTime64(3)", utc, "2020-01-01 00:00:00.120"},
                {"DateTime('Asia/Tokyo')", utc.withZoneSameInstant(ZoneId.of("Asia/Tokyo")), "2020-01-01 09:00:00"},
                {"DateTime64(6)", Timestamp.valueOf(LocalDateTime.of(2020, 1, 1, 0, 0, 0, 5000)), "2020-01-01 00:00:00.000005"},
                {"Array(Date)", Collections.singletonList(LocalDate.of(2020, 1, 2)), "['2020-01-02']"},
                {"Date32", java.sql.Date.valueOf(LocalDate.of(1900, 1, 1)), "1900-01-01"},
                // The server writes a Date32 / DateTime64 after year 9999 or before year 0 as the bound, with the
                // fractional digits of the value.
                {"Date32", LocalDate.of(5879610, 7, 12), "9999-12-31"},
                {"Date32", LocalDate.of(-1, 12, 31), "0000-01-01"},
                {"Date32", LocalDate.of(0, 1, 4), "0000-01-04"},
                {"DateTime64(3, 'UTC')", Instant.ofEpochSecond(1234567891011L, 123_000_000).atZone(ZoneOffset.UTC),
                        "9999-12-31 23:59:59.123"},
                {"DateTime64(3, 'UTC')", Instant.ofEpochSecond(-62167219201L, 123_000_000).atZone(ZoneOffset.UTC),
                        "0000-01-01 00:00:00.123"},
                {"DateTime64(0, 'UTC')", Instant.ofEpochSecond(253402300799L).atZone(ZoneOffset.UTC),
                        "9999-12-31 23:59:59"},
                {"DateTime64(0, 'Asia/Makassar')",
                        Instant.ofEpochSecond(253402300799L).atZone(ZoneId.of("Asia/Makassar")), "9999-12-31 23:59:59"},
                {"DateTime64(6)", Timestamp.valueOf(LocalDateTime.of(41091, 11, 25, 5, 16, 51, 5000)),
                        "9999-12-31 23:59:59.000005"},
                // client-v2 reads Time and Time64 as a LocalDateTime on 1970-01-01.
                {"Time", LocalDateTime.ofEpochSecond(-3723, 0, ZoneOffset.UTC), "-01:02:03"},
                {"Time64(3)", LocalDateTime.ofEpochSecond(360000, 500_000_000, ZoneOffset.UTC), "100:00:00.500"},
                {"Time64(3)", LocalDateTime.ofEpochSecond(-2, 500_000_000, ZoneOffset.UTC), "-00:00:01.500"},
                // jdbc-v2 reads Time and Time64 as java.sql.Time, which holds milliseconds.
                {"Time64(3)", new java.sql.Time(java.sql.Time.valueOf("10:10:10").getTime() + 111), "10:10:10.111"},
                {"Time64(6)", new java.sql.Time(java.sql.Time.valueOf("00:00:01").getTime() + 5), "00:00:01.005000"},
                {"Time", java.sql.Time.valueOf("23:59:59"), "23:59:59"},
                // The server writes a value above 999:59:59 as 999:59:59.
                {"Time64(2)", Duration.ofHours(2160).plusSeconds(12), "999:59:59.00"},
                {"Time", Duration.ofHours(-2000), "-999:59:59"},
                {"UUID", UUID.fromString("61f0c404-5cb3-11e7-907b-a6006ad3dba0"), "61f0c404-5cb3-11e7-907b-a6006ad3dba0"},
                {"IPv4", InetAddress.getByName("1.2.3.4"), "1.2.3.4"},
                {"Array(IPv6)", Collections.singletonList(InetAddress.getByName("::ffff:1.2.3.4")), "['::ffff:1.2.3.4']"},
                {"Bool", true, "true"},
                {"Array(Float64)", new double[] {Double.NaN, Double.POSITIVE_INFINITY}, "[nan,inf]"},
                {"IntervalDay", Period.ofDays(3), "3"},
                {"IntervalWeek", Period.ofWeeks(2), "2"},
                {"IntervalSecond", Duration.ofSeconds(-5), "-5"},
                {"LowCardinality(String)", "a", "a"},
                {"SimpleAggregateFunction(sum, UInt64)", BigInteger.valueOf(7), "7"},
                {"Enum8('a' = 1, 'b' = 2)", new BinaryStreamReader.EnumValue("b", 2), "b"},
                {"Array(Enum8('a' = 1))", Collections.singletonList("a"), "['a']"},
                {"Point", new double[] {1.5, 2.5}, "(1.5,2.5)"},
                {"Ring", new double[][] {{1, 2}, {3, 4}}, "[(1,2),(3,4)]"},
                // A Geometry value is written as its active member, selected by the number of array dimensions.
                {"Geometry", new double[] {1.5, 2}, "(1.5,2)"},
                {"Geometry", new double[][] {{1, 2}, {3, 4}}, "[(1,2),(3,4)]"},
                {"Geometry", new double[][][] {{{1, 2}, {3, 4}}, {}}, "[[(1,2),(3,4)],[]]"},
                {"Geometry", new double[][][][] {{{{0.5, 1e-7}}}}, "[[[(0.5,1e-7)]]]"},
                {"Geometry", new double[][][] {}, "[]"},
                {"Geometry", null, "\\N"},
                {"Variant(String, UInt64)", BigInteger.ONE, "1"},
                {"Array(Variant(String, UInt64))", Arrays.asList("a", BigInteger.ONE, null), "['a',1,NULL]"},
                {"Nothing", null, "\\N"},
                {"Nested(a UInt8, b String)", Collections.singletonList(new Object[] {(short) 1, "x"}), "[(1,'x')]"},
        };
    }

    @Test(dataProvider = "values")
    public void testRender(String type, Object value, String expected) {
        assertEquals(render(type, value), expected);
    }

    @Test
    public void testRenderRaw() {
        TsvOutput out = new TsvOutput();
        new TsvRenderer("Nullable(String)").render(out, "a\tb", true);
        assertEquals(out.toString(), "a\tb");
    }

    @Test
    public void testRenderUnsupported() {
        // A map without order cannot be written.
        Map<String, Short> unordered = new HashMap<>();
        unordered.put("a", (short) 1);
        unordered.put("b", (short) 2);
        assertThrows(TsvRenderer.UnsupportedTypeException.class, () -> render("Map(String, UInt8)", unordered));
        assertThrows(TsvRenderer.UnsupportedTypeException.class, () -> render("Dynamic", 1));
        assertThrows(TsvRenderer.UnsupportedTypeException.class, () -> render("JSON", Collections.emptyMap()));
        // A Java value that does not fit the type is an error, not a guess.
        assertThrows(TsvRenderer.ValueMismatchException.class, () -> render("Float32", 1.5d));
        assertThrows(TsvRenderer.ValueMismatchException.class, () -> render("DateTime", LocalDate.of(2020, 1, 1)));
        assertThrows(TsvRenderer.ValueMismatchException.class, () -> render("Tuple(UInt8, UInt8)", new Object[] {(short) 1}));
        assertThrows(TsvRenderer.ValueMismatchException.class, () -> render("Geometry", "POINT (1 2)"));
        assertThrows(TsvRenderer.ValueMismatchException.class, () -> render("Geometry", new double[][][][][] {}));
    }
}

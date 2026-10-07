package com.clickhouse.client.testdata;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet6Address;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static com.clickhouse.client.testdata.DataTypeGenerators.*;

public class DataTypeGeneratorsTest {

    @DataProvider
    public Object[][] sqlLiterals() throws Exception {
        byte[] mapped = new byte[16];
        mapped[10] = (byte) 0xFF;
        mapped[11] = (byte) 0xFF;
        mapped[12] = 127;
        mapped[15] = 1;
        Map<String, Object> mapValue = new LinkedHashMap<>();
        mapValue.put("k", Arrays.asList((short) 1, (short) 2));
        return new Object[][] {
                {string(), "it's \\ \n", "'it\\'s \\\\ \\x0A'"},
                {binaryString(), new byte[] {0, (byte) 0xFF}, "'\\x00\\xFF'"},
                {fixedString(3), "a\0\0", "'a\\x00\\x00'"},
                {float64(), Double.NaN, "nan"},
                {float32(), Float.NEGATIVE_INFINITY, "-inf"},
                {decimal(10, 3), new BigDecimal("-1.500"), "-1.500"},
                {uint256(), BigInteger.ONE.shiftLeft(256).subtract(BigInteger.ONE), BigInteger.ONE.shiftLeft(256).subtract(BigInteger.ONE).toString()},
                {date32(), LocalDate.of(1900, 1, 1), "'1900-01-01'"},
                {time64(3), Duration.ofSeconds(3723, 400_000_000).negated(), "'-01:02:03.400'"},
                {time(), Duration.ofSeconds(999 * 3600 + 59 * 60 + 59), "'999:59:59'"},
                {dateTime("Asia/Tokyo"), Instant.ofEpochSecond(1).atZone(ZoneId.of("Asia/Tokyo")), "1"},
                {dateTime64(6, "Asia/Tokyo"), Instant.parse("1900-01-01T00:00:00.000001Z").atZone(ZoneId.of("Asia/Tokyo")),
                        "toDateTime64('1900-01-01 00:00:00.000001', 6, 'UTC')"},
                {ipv6(), Inet6Address.getByAddress(null, mapped, -1), "'::ffff:127.0.0.1'"},
                {enum8(), "quote'", "'quote\\''"},
                {enumeration("Enum16", Collections.singletonMap("a", -1000), true), "a", "-1000"},
                {interval("Quarter"), Period.ofMonths(6), "toIntervalQuarter(2)"},
                {interval("Microsecond"), Duration.ofNanos(-5000), "toIntervalMicrosecond(-5)"},
                {polygon(), new double[][][] {{{1, -0.0}}}, "[[(1.0, -0.0)]]"},
                {nullable(int32()), null, "NULL"},
                {array(nullable(string())), Arrays.asList("a", null), "['a', NULL]"},
                {tuple(int8()), Collections.singletonList((byte) 1), "tuple(1)"},
                {tuple(bool(), uuid()), Arrays.asList(true, new UUID(0, 1)),
                        "(true, '00000000-0000-0000-0000-000000000001')"},
                {map(string(), array(uint8())), mapValue, "{'k': [1, 2]}"},
        };
    }

    @Test(groups = {"unit"}, dataProvider = "sqlLiterals")
    public void testSqlLiteral(DataTypeGenerator generator, Object value, String expected) {
        Assert.assertEquals(generator.toSqlLiteral(value), expected);
    }

    /**
     * Expected values are the output of ClickHouse 26.10 for {@code SELECT ... FORMAT TSV}.
     */
    @DataProvider
    public Object[][] tsvValues() throws Exception {
        byte[] compatible = new byte[16];
        compatible[12] = 127;
        compatible[15] = 1;
        Map<String, Object> mapValue = new LinkedHashMap<>();
        mapValue.put("k", Arrays.asList((short) 1, (short) 2));
        return new Object[][] {
                {float64(), -0d, "-0", "-0"},
                {float64(), 1e20, "100000000000000000000", "100000000000000000000"},
                {float64(), 1e21, "1e21", "1e21"},
                {float64(), 1e-5, "0.00001", "0.00001"},
                {float64(), 1e-7, "1e-7", "1e-7"},
                {float64(), 0.1 + 0.2, "0.30000000000000004", "0.30000000000000004"},
                {float64(), Double.MIN_VALUE, "5e-324", "5e-324"},
                {float64(), 9.223372036854776e18, "9223372036854776000", "9223372036854776000"},
                {float64(), Double.NEGATIVE_INFINITY, "-inf", "-inf"},
                {float32(), Float.MAX_VALUE, "3.4028235e38", "3.4028235e38"},
                {float32(), 0.1f, "0.1", "0.1"},
                {float32(), Float.MIN_VALUE, "1e-45", "1e-45"},
                {decimal(10, 3), new BigDecimal("1.500"), "1.5", "1.5"},
                {decimal(10, 3), new BigDecimal("0.000"), "0", "0"},
                {decimal(76, 2), new BigDecimal("-99.00"), "-99", "-99"},
                {string(), "a\tb\nc\\d'e\0f\u0001\b\f\r", "a\\tb\\nc\\\\d\\'e\\0f\u0001\\b\\f\\r",
                        "'a\\tb\\nc\\\\d\\'e\\0f\u0001\\b\\f\\r'"},
                {string(), "ж", "Ð¶", "'Ð¶'"},
                {binaryString(), new byte[] {(byte) 0xFF, 0}, "ÿ\\0", "'ÿ\\0'"},
                {fixedString(3), "a\0\0", "a\\0\\0", "'a\\0\\0'"},
                {time(), Duration.ofSeconds(-3723), "-01:02:03", "'-01:02:03'"},
                {time64(9), Duration.ofNanos(-1), "-00:00:00.000000001", "'-00:00:00.000000001'"},
                {dateTime("UTC"), Instant.EPOCH.atZone(ZoneId.of("UTC")), "1970-01-01 00:00:00",
                        "'1970-01-01 00:00:00'"},
                {dateTime64(3, "Asia/Kolkata"), Instant.parse("1900-01-01T00:00:00.5Z").atZone(ZoneId.of("Asia/Kolkata")),
                        "1900-01-01 05:21:10.500", "'1900-01-01 05:21:10.500'"},
                {ipv6(), Inet6Address.getByAddress(null, new byte[16], -1), "::", "'::'"},
                {ipv6(), Inet6Address.getByAddress(null, compatible, -1), "::127.0.0.1", "'::127.0.0.1'"},
                {ipv6(), Inet6Address.getByName("1:0:0:2::3"), "1:0:0:2::3", "'1:0:0:2::3'"},
                {ipv6(), Inet6Address.getByName("1:0:2:3:4:5:6:7"), "1:0:2:3:4:5:6:7", "'1:0:2:3:4:5:6:7'"},
                {ipv6(), Inet6Address.getByName("ABCD::"), "abcd::", "'abcd::'"},
                {uuid(), new UUID(0, 0xABCD), "00000000-0000-0000-0000-00000000abcd",
                        "'00000000-0000-0000-0000-00000000abcd'"},
                {enum8(), "quote'", "quote\\'", "'quote\\''"},
                {interval("Quarter"), Period.ofMonths(-6), "-2", "-2"},
                {nothing(), null, "\\N", "NULL"},
                {point(), new double[] {1.5, -0.0}, "(1.5,-0)", "(1.5,-0)"},
                {nullable(int32()), null, "\\N", "NULL"},
                {lowCardinality(nullable(string())), "x", "x", "'x'"},
                {array(nullable(string())), Arrays.asList("a\t'", null), "['a\\t\\'',NULL]", "['a\\t\\'',NULL]"},
                {tuple(int8()), Collections.singletonList((byte) 1), "(1)", "(1)"},
                {tuple(bool(), date()), Arrays.asList(true, LocalDate.of(2020, 1, 1)), "(true,'2020-01-01')",
                        "(true,'2020-01-01')"},
                {map(string(), array(uint8())), mapValue, "{'k':[1,2]}", "{'k':[1,2]}"},
        };
    }

    @Test(groups = {"unit"}, dataProvider = "tsvValues")
    public void testTsv(DataTypeGenerator generator, Object value, String expectedTsv, String expectedQuoted) {
        Assert.assertEquals(generator.toTsv(value), expectedTsv);
        Assert.assertEquals(generator.toQuotedText(value), expectedQuoted);
    }

    @DataProvider
    public Object[][] typeNames() {
        Map<String, DataTypeGenerator> fields = new LinkedHashMap<>();
        fields.put("a.b", int32());
        fields.put("we`ird", lowCardinality(string()));
        return new Object[][] {
                {decimal(76, 38), "Decimal(76, 38)"},
                {dateTime64(9, "America/New_York"), "DateTime64(9, 'America/New_York')"},
                {enumeration("Enum8", Collections.singletonMap("a'b", 1), false), "Enum8('a\\'b' = 1)"},
                {map(lowCardinality(string()), nullable(ipv4())), "Map(LowCardinality(String), Nullable(IPv4))"},
                {array(array(nothing())), "Array(Array(Nullable(Nothing)))"},
                {namedTuple(fields), "Tuple(`a.b` Int32, `we\\`ird` LowCardinality(String))"},
                {nested(fields), "Nested(`a.b` Int32, `we\\`ird` LowCardinality(String))"},
        };
    }

    @Test(groups = {"unit"}, dataProvider = "typeNames")
    public void testTypeName(DataTypeGenerator generator, String expected) {
        Assert.assertEquals(generator.getType(), expected);
    }

    @DataProvider
    public Object[][] expectedValues() {
        long maxTime = 999 * 3600 + 59 * 60 + 59;
        return new Object[][] {
                {uint8(), new Object[] {(short) 0, (short) 255}},
                {int16(), new Object[] {Short.MIN_VALUE, (short) -1, Short.MAX_VALUE}},
                {uint32(), new Object[] {0L, 4294967295L, 2147483648L}},
                {int64(), new Object[] {Long.MIN_VALUE, Long.MAX_VALUE, 0L, -1L, 4294967296L}},
                {uint256(), new Object[] {BigInteger.ZERO, BigInteger.ONE.shiftLeft(256).subtract(BigInteger.ONE)}},
                {float32(), new Object[] {Float.NaN, -0f, Float.MIN_VALUE, Float.MAX_VALUE}},
                {decimal(5, 2), new Object[] {new BigDecimal("999.99"), new BigDecimal("-999.99"), new BigDecimal("0.01")}},
                {decimal(3, 3), new Object[] {new BigDecimal("0.999"), new BigDecimal("-0.001")}},
                {fixedString(3), new Object[] {"a\0\0", "\0\0\0", "zzz"}},
                {date(), new Object[] {LocalDate.of(1970, 1, 1), LocalDate.of(1972, 2, 29), LocalDate.of(2149, 6, 6)}},
                {date32(), new Object[] {LocalDate.of(1900, 1, 1), LocalDate.of(1969, 12, 31), LocalDate.of(2299, 12, 31)}},
                {time(), new Object[] {Duration.ofSeconds(-maxTime), Duration.ofSeconds(maxTime), Duration.ofSeconds(-1)}},
                {time64(9), new Object[] {Duration.ofSeconds(maxTime, 999_999_999), Duration.ofNanos(-1)}},
                {dateTime("UTC"), new Object[] {Instant.EPOCH.atZone(ZoneId.of("UTC")),
                        Instant.ofEpochSecond(4294967295L).atZone(ZoneId.of("UTC"))}},
                {dateTime("America/New_York"), new Object[] {
                        Instant.parse("2021-03-14T07:00:00Z").atZone(ZoneId.of("America/New_York")),
                        Instant.parse("2021-11-07T06:00:00Z").atZone(ZoneId.of("America/New_York"))}},
                {dateTime64(9, "UTC"), new Object[] {Instant.parse("1900-01-01T00:00:00Z").atZone(ZoneId.of("UTC")),
                        Instant.parse("2262-04-11T23:47:16.854775807Z").atZone(ZoneId.of("UTC"))}},
                {dateTime64(2, "UTC"), new Object[] {Instant.parse("2299-12-31T23:59:59.99Z").atZone(ZoneId.of("UTC"))}},
                {nullable(bool()), new Object[] {null, true, false}},
                {array(int8()), new Object[] {Collections.emptyList(), Collections.singletonList(Byte.MIN_VALUE)}},
                {nothing(), new Object[] {null}},
        };
    }

    @Test(groups = {"unit"}, dataProvider = "expectedValues")
    public void testGeneratedValues(DataTypeGenerator generator, Object[] expected) {
        List<Object> values = generator.generate(new Random(1));
        for (Object value : expected) {
            Assert.assertTrue(values.contains(value), generator + " has no value " + value);
        }
    }

    @DataProvider
    public Object[][] exhaustiveTypes() {
        return new Object[][] {{int8(), 256}, {uint8(), 256}, {int16(), 65536}, {uint16(), 65536}, {bool(), 2}};
    }

    @Test(groups = {"unit"}, dataProvider = "exhaustiveTypes")
    public void testExhaustiveTypes(DataTypeGenerator generator, int expectedSize) {
        Assert.assertEquals(new HashSet<>(generator.generate(new Random(1))).size(), expectedSize);
    }

    @Test(groups = {"unit"})
    public void testCompositesCoverAllElementValues() {
        List<Object> elements = int8().generate(new Random(1));
        HashSet<Object> fromArrays = new HashSet<>();
        for (Object array : array(int8()).generate(new Random(1))) {
            fromArrays.addAll((List<?>) array);
        }
        HashSet<Object> fromMaps = new HashSet<>();
        for (Object value : map(int8(), bool()).generate(new Random(1))) {
            fromMaps.addAll(((Map<?, ?>) value).keySet());
        }
        Assert.assertEquals(fromArrays, new HashSet<>(elements));
        Assert.assertEquals(fromMaps, new HashSet<>(elements));
    }

    @Test(groups = {"unit"})
    public void testDataSetSql() {
        Map<String, Integer> constants = new LinkedHashMap<>();
        constants.put("a", 1);
        constants.put("b", 2);
        constants.put("c", 3);
        TestDataSet dataSet = TestDataSet.builder()
                .column("flag", bool())
                .column("e", enumeration("Enum8", constants, false))
                .build();

        Assert.assertEquals(dataSet.getCreateTableSql("t"),
                "CREATE TABLE t (id Int32, `flag` Bool, `e` Enum8('a' = 1, 'b' = 2, 'c' = 3)) ENGINE = MergeTree ORDER BY id");
        Assert.assertEquals(dataSet.getInsertSql("t"),
                "INSERT INTO t (id, `flag`, `e`) VALUES (0, true, 'a'),\n(1, false, 'b'),\n(2, true, 'c')");
        Assert.assertEquals(dataSet.getTsv(), "0\ttrue\ta\n1\tfalse\tb\n2\ttrue\tc\n");
        Assert.assertTrue(Arrays.deepEquals(dataSet.toDataProvider(),
                new Object[][] {{0, true, "a"}, {1, false, "b"}, {2, true, "c"}}));
    }

    @Test(groups = {"unit"})
    public void testDataSetIsReproducible() {
        TestDataSet dataSet = TestDataSet.builder().seed(7).column("v", float64()).column("s", string()).build();

        Assert.assertEquals(TestDataSet.builder().seed(7).column("v", float64()).column("s", string()).build()
                .getValuesClause(), dataSet.getValuesClause());
        Assert.assertNotEquals(TestDataSet.builder().seed(8).column("v", float64()).column("s", string()).build()
                .getValuesClause(), dataSet.getValuesClause());
    }

    @Test(groups = {"unit"})
    public void testDataSetRequiredSettings() {
        Map<String, DataTypeGenerator> fields = Collections.singletonMap("t", lowCardinality(nullable(time())));
        TestDataSet dataSet = TestDataSet.of(nested(fields));

        Assert.assertEquals(dataSet.getRequiredSettings().get("enable_time_time64_type"), "1");
        Assert.assertEquals(dataSet.getRequiredSettings().get("allow_suspicious_low_cardinality_types"), "1");
        Assert.assertEquals(dataSet.getRequiredSettings().get("flatten_nested"), "0");
    }

    @Test(groups = {"unit"})
    public void testInvalidUsage() {
        Assert.expectThrows(IllegalStateException.class,
                () -> TestDataSet.of(array(interval("Day"))).getCreateTableSql("t"));
        Assert.expectThrows(IllegalArgumentException.class,
                () -> TestDataSet.builder().column(TestDataSet.ID_COLUMN, int8()));
        Assert.expectThrows(IllegalArgumentException.class, () -> decimal(10, 11));
        Assert.expectThrows(IllegalArgumentException.class, () -> dateTime64(10, "UTC"));
        Assert.expectThrows(IllegalArgumentException.class, () -> interval("Century"));
    }
}

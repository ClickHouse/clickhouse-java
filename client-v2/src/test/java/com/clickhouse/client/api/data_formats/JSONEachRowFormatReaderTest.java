package com.clickhouse.client.api.data_formats;

import com.clickhouse.client.api.data_formats.internal.JsonParser;
import com.clickhouse.data.ClickHouseDataType;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

@Test(groups = {"unit"})
public class JSONEachRowFormatReaderTest {

    /** Simple in-memory parser that yields a fixed list of rows. */
    private static final class StubJsonParser implements JsonParser {
        private final List<Map<String, Object>> rows;
        private boolean closed;
        private int index;

        StubJsonParser(List<Map<String, Object>> rows) {
            this.rows = new ArrayList<>(rows);
        }

        @Override
        public Map<String, Object> nextRow() {
            return index < rows.size() ? rows.get(index++) : null;
        }

        @Override
        public void close() {
            closed = true;
        }

        boolean isClosed() {
            return closed;
        }
    }

    private static Map<String, Object> row(Object... pairs) {
        Map<String, Object> r = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            r.put((String) pairs[i], pairs[i + 1]);
        }
        return r;
    }

    private static JSONEachRowFormatReader readerOf(Map<String, Object>... rows) {
        return new JSONEachRowFormatReader(new StubJsonParser(Arrays.asList(rows)));
    }

    // ---------------------------------------------------------------------
    // guessDataType
    // ---------------------------------------------------------------------

    @Test
    public void testGuessDataTypeForIntegerLikeValuesIsInt64() {
        JSONEachRowFormatReader reader = readerOf(row(
                "as_integer", 1,
                "as_long", 2L,
                "as_big_integer", BigInteger.TEN));

        Assert.assertEquals(reader.getSchema().getColumnByName("as_integer").getDataType(),
                ClickHouseDataType.Int64);
        Assert.assertEquals(reader.getSchema().getColumnByName("as_long").getDataType(),
                ClickHouseDataType.Int64);
        Assert.assertEquals(reader.getSchema().getColumnByName("as_big_integer").getDataType(),
                ClickHouseDataType.Int64);
    }

    @Test
    public void testGuessDataTypeForFractionalDoubleIsFloat64() {
        JSONEachRowFormatReader reader = readerOf(row(
                "as_double", 1.5d,
                "as_float", 2.5f,
                "as_big_decimal", new BigDecimal("3.14")));

        Assert.assertEquals(reader.getSchema().getColumnByName("as_double").getDataType(),
                ClickHouseDataType.Float64);
        Assert.assertEquals(reader.getSchema().getColumnByName("as_float").getDataType(),
                ClickHouseDataType.Float64);
        Assert.assertEquals(reader.getSchema().getColumnByName("as_big_decimal").getDataType(),
                ClickHouseDataType.Float64);
    }

    @Test
    public void testGuessDataTypeForWholeDoubleIsInt64() {
        JSONEachRowFormatReader reader = readerOf(row(
                "as_double_whole", 5.0d,
                "as_float_whole", 7.0f,
                "as_big_decimal_whole", new BigDecimal("42")));

        Assert.assertEquals(reader.getSchema().getColumnByName("as_double_whole").getDataType(),
                ClickHouseDataType.Int64);
        Assert.assertEquals(reader.getSchema().getColumnByName("as_float_whole").getDataType(),
                ClickHouseDataType.Int64);
        Assert.assertEquals(reader.getSchema().getColumnByName("as_big_decimal_whole").getDataType(),
                ClickHouseDataType.Int64);
    }

    @Test
    public void testGuessDataTypeForOutOfRangeDoubleIsFloat64() {
        // Values outside the long range cannot be represented as Int64; the
        // reader must fall back to Float64 even when they are mathematically
        // whole numbers.
        JSONEachRowFormatReader reader = readerOf(row(
                "too_big", 1.0e20d,
                "infinite", Double.POSITIVE_INFINITY));

        Assert.assertEquals(reader.getSchema().getColumnByName("too_big").getDataType(),
                ClickHouseDataType.Float64);
        Assert.assertEquals(reader.getSchema().getColumnByName("infinite").getDataType(),
                ClickHouseDataType.Float64);
    }

    @Test
    public void testGuessDataTypeForOtherNumberSubtypesIsFloat64() {
        // AtomicInteger is a Number that is neither Integer/Long/BigInteger
        // nor Double/Float/BigDecimal, so it lands in the catch-all numeric
        // branch.
        JSONEachRowFormatReader reader = readerOf(row("custom", new AtomicInteger(5)));
        Assert.assertEquals(reader.getSchema().getColumnByName("custom").getDataType(),
                ClickHouseDataType.Float64);
    }

    @Test
    public void testGuessDataTypeForBooleanIsBool() {
        JSONEachRowFormatReader reader = readerOf(row("flag", Boolean.TRUE));
        Assert.assertEquals(reader.getSchema().getColumnByName("flag").getDataType(),
                ClickHouseDataType.Bool);
    }

    @Test
    public void testGuessDataTypeDefaultBranchIsString() {
        // Strings, lists, maps, and JSON null should all fall through to the
        // catch-all branch and be reported as String columns.
        JSONEachRowFormatReader reader = readerOf(row(
                "as_string", "hello",
                "as_list", Arrays.asList(1, 2, 3),
                "as_map", Collections.singletonMap("k", "v"),
                "as_null", null));

        Assert.assertEquals(reader.getSchema().getColumnByName("as_string").getDataType(),
                ClickHouseDataType.String);
        Assert.assertEquals(reader.getSchema().getColumnByName("as_list").getDataType(),
                ClickHouseDataType.String);
        Assert.assertEquals(reader.getSchema().getColumnByName("as_map").getDataType(),
                ClickHouseDataType.String);
        Assert.assertEquals(reader.getSchema().getColumnByName("as_null").getDataType(),
                ClickHouseDataType.String);
    }

    @Test
    public void testEmptyResultSetExposesEmptySchema() {
        JSONEachRowFormatReader reader = new JSONEachRowFormatReader(
                new StubJsonParser(Collections.emptyList()));

        Assert.assertNotNull(reader.getSchema());
        Assert.assertEquals(reader.getSchema().getColumns().size(), 0);
        Assert.assertFalse(reader.hasNext());
        Assert.assertNull(reader.next());
    }

    @Test
    public void testReaderInitializationWrapsParserFailures() {
        JsonParser failing = new JsonParser() {
            @Override
            public Map<String, Object> nextRow() throws Exception {
                throw new IllegalStateException("boom");
            }
            @Override
            public void close() { }
        };
        try {
            new JSONEachRowFormatReader(failing);
            Assert.fail("Expected RuntimeException");
        } catch (RuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("Failed to initialize JSON reader"),
                    "Unexpected message: " + e.getMessage());
            Assert.assertTrue(e.getCause() instanceof IllegalStateException);
        }
    }

    @Test
    public void testNextRowFailureIsWrapped() throws Exception {
        JsonParser parser = new JsonParser() {
            private int call;
            @Override
            public Map<String, Object> nextRow() {
                if (call++ == 0) {
                    return row("id", 1);
                }
                throw new IllegalStateException("kaboom");
            }
            @Override
            public void close() { }
        };
        try (JSONEachRowFormatReader reader = new JSONEachRowFormatReader(parser)) {
            // First row was read eagerly during construction.
            Assert.assertNotNull(reader.next());
            try {
                reader.next();
                Assert.fail("Expected RuntimeException");
            } catch (RuntimeException e) {
                Assert.assertTrue(e.getMessage().contains("Failed to read next JSON row"),
                        "Unexpected message: " + e.getMessage());
            }
        }
    }

    // ---------------------------------------------------------------------
    // Row navigation, readValue, hasValue
    // ---------------------------------------------------------------------

    @Test
    public void testHasNextAndNext() throws Exception {
        Map<String, Object> r1 = row("id", 1);
        Map<String, Object> r2 = row("id", 2);

        try (JSONEachRowFormatReader reader = readerOf(r1, r2)) {
            Assert.assertTrue(reader.hasNext());
            Assert.assertSame(reader.next(), r1);
            // After the first row has been returned, hasNext() optimistically
            // returns true; callers detect the end of the stream when next()
            // returns null.
            Assert.assertTrue(reader.hasNext());
            Assert.assertSame(reader.next(), r2);
            Assert.assertNull(reader.next());
        }
    }

    @Test
    public void testReadValueByIndexAndName() throws Exception {
        try (JSONEachRowFormatReader reader = readerOf(row("id", 42, "name", "abc"))) {
            reader.next();
            Number byIndex = reader.readValue(1);
            Assert.assertNotNull(byIndex);
            Assert.assertEquals(byIndex.intValue(), 42);
            Assert.assertEquals((String) reader.readValue("name"), "abc");
            Assert.assertEquals((String) reader.readValue(2), "abc");
        }
    }

    @Test
    public void testHasValue() throws Exception {
        Map<String, Object> r = new HashMap<>();
        r.put("present", "value");
        r.put("nullable", null);

        try (JSONEachRowFormatReader reader = new JSONEachRowFormatReader(
                new StubJsonParser(Collections.singletonList(r)))) {
            reader.next();
            Assert.assertTrue(reader.hasValue("present"));
            Assert.assertFalse(reader.hasValue("nullable"));
            Assert.assertFalse(reader.hasValue("missing"));
            // The schema only contains the keys observed in the first row, so
            // any column index resolved to a name that is present must be
            // truthy and any nullable column must be falsy.
            Assert.assertEquals(reader.hasValue(1), reader.hasValue(reader.getSchema().columnIndexToName(1)));
        }
    }

    @Test
    public void testCloseDelegatesToParser() throws Exception {
        StubJsonParser parser = new StubJsonParser(Collections.singletonList(row("id", 1)));
        JSONEachRowFormatReader reader = new JSONEachRowFormatReader(parser);
        Assert.assertFalse(parser.isClosed());
        reader.close();
        Assert.assertTrue(parser.isClosed());
    }

    // ---------------------------------------------------------------------
    // Typed accessors
    // ---------------------------------------------------------------------

    @Test
    public void testNumericAccessors() throws Exception {
        try (JSONEachRowFormatReader reader = readerOf(row(
                "b", 120,
                "s", 30000,
                "i", 1_000_000,
                "l", 10_000_000_000L,
                "f", 1.5d,
                "d", 2.5d))) {
            reader.next();

            Assert.assertEquals(reader.getByte("b"), (byte) 120);
            Assert.assertEquals(reader.getByte(1), (byte) 120);
            Assert.assertEquals(reader.getShort("s"), (short) 30000);
            Assert.assertEquals(reader.getShort(2), (short) 30000);
            Assert.assertEquals(reader.getInteger("i"), 1_000_000);
            Assert.assertEquals(reader.getInteger(3), 1_000_000);
            Assert.assertEquals(reader.getLong("l"), 10_000_000_000L);
            Assert.assertEquals(reader.getLong(4), 10_000_000_000L);
            Assert.assertEquals(reader.getFloat("f"), 1.5f, 0.0001f);
            Assert.assertEquals(reader.getFloat(5), 1.5f, 0.0001f);
            Assert.assertEquals(reader.getDouble("d"), 2.5d, 0.0001d);
            Assert.assertEquals(reader.getDouble(6), 2.5d, 0.0001d);

            Assert.assertEquals(reader.getEnum8("b"), (byte) 120);
            Assert.assertEquals(reader.getEnum8(1), (byte) 120);
            Assert.assertEquals(reader.getEnum16("s"), (short) 30000);
            Assert.assertEquals(reader.getEnum16(2), (short) 30000);
        }
    }

    @Test
    public void testStringAccessor() throws Exception {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("s", "hello");
        r.put("missing", null);

        try (JSONEachRowFormatReader reader = new JSONEachRowFormatReader(
                new StubJsonParser(Collections.singletonList(r)))) {
            reader.next();
            Assert.assertEquals(reader.getString("s"), "hello");
            Assert.assertEquals(reader.getString(1), "hello");
            Assert.assertNull(reader.getString("missing"));
            Assert.assertNull(reader.getString(2));
        }
    }

    @Test
    public void testBooleanAccessor() throws Exception {
        try (JSONEachRowFormatReader reader = readerOf(row(
                "from_bool", Boolean.TRUE,
                "from_zero", 0,
                "from_nonzero", 1,
                "from_string", "true"))) {
            reader.next();
            Assert.assertTrue(reader.getBoolean("from_bool"));
            Assert.assertFalse(reader.getBoolean("from_zero"));
            Assert.assertTrue(reader.getBoolean("from_nonzero"));
            Assert.assertTrue(reader.getBoolean("from_string"));
            Assert.assertTrue(reader.getBoolean(1));
        }
    }

    @Test
    public void testBigNumberAccessors() throws Exception {
        try (JSONEachRowFormatReader reader = readerOf(row(
                "from_big_integer", new BigInteger("123456789012345"),
                "from_string_int", "987654321",
                "from_big_decimal", new BigDecimal("12345.6789"),
                "from_string_dec", "0.5"))) {
            reader.next();

            Assert.assertEquals(reader.getBigInteger("from_big_integer"),
                    new BigInteger("123456789012345"));
            Assert.assertEquals(reader.getBigInteger("from_string_int"),
                    new BigInteger("987654321"));
            Assert.assertEquals(reader.getBigInteger(1), new BigInteger("123456789012345"));
            Assert.assertNull(reader.getBigInteger("not_a_column"));

            Assert.assertEquals(reader.getBigDecimal("from_big_decimal").compareTo(new BigDecimal("12345.6789")), 0);
            Assert.assertEquals(reader.getBigDecimal("from_string_dec").compareTo(new BigDecimal("0.5")), 0);
            Assert.assertEquals(reader.getBigDecimal(3).compareTo(new BigDecimal("12345.6789")), 0);
        }
    }

    @Test
    public void testTemporalAccessors() throws Exception {
        try (JSONEachRowFormatReader reader = readerOf(row(
                "d", "2024-05-06",
                "t", "07:08:09",
                "dt", "2024-05-06T07:08:09",
                "odt", "2024-05-06T07:08:09+02:00"))) {
            reader.next();

            Assert.assertEquals(reader.getLocalDate("d"), java.time.LocalDate.of(2024, 5, 6));
            Assert.assertEquals(reader.getLocalDate(1), java.time.LocalDate.of(2024, 5, 6));
            Assert.assertEquals(reader.getLocalTime("t"), java.time.LocalTime.of(7, 8, 9));
            Assert.assertEquals(reader.getLocalTime(2), java.time.LocalTime.of(7, 8, 9));
            Assert.assertEquals(reader.getLocalDateTime("dt"),
                    java.time.LocalDateTime.of(2024, 5, 6, 7, 8, 9));
            Assert.assertEquals(reader.getLocalDateTime(3),
                    java.time.LocalDateTime.of(2024, 5, 6, 7, 8, 9));
            Assert.assertEquals(reader.getOffsetDateTime("odt"),
                    java.time.OffsetDateTime.parse("2024-05-06T07:08:09+02:00"));
            Assert.assertEquals(reader.getOffsetDateTime(4),
                    java.time.OffsetDateTime.parse("2024-05-06T07:08:09+02:00"));
        }
    }

    @Test
    public void testUuidAndListAccessors() throws Exception {
        UUID uuid = UUID.fromString("11111111-2222-3333-4444-555555555555");
        try (JSONEachRowFormatReader reader = readerOf(row(
                "u", uuid.toString(),
                "arr", Arrays.asList(1, 2, 3)))) {
            reader.next();

            Assert.assertEquals(reader.getUUID("u"), uuid);
            Assert.assertEquals(reader.getUUID(1), uuid);

            List<Integer> list = reader.getList("arr");
            Assert.assertEquals(list, Arrays.asList(1, 2, 3));
            Assert.assertEquals(reader.<Integer>getList(2), Arrays.asList(1, 2, 3));
        }
    }

    // ---------------------------------------------------------------------
    // Unsupported operations
    // ---------------------------------------------------------------------

    @Test
    public void testUnsupportedAccessorsThrow() throws Exception {
        try (JSONEachRowFormatReader reader = readerOf(row("v", "x"))) {
            reader.next();

            assertUnsupported(() -> reader.getInstant("v"));
            assertUnsupported(() -> reader.getInstant(1));
            assertUnsupported(() -> reader.getZonedDateTime("v"));
            assertUnsupported(() -> reader.getZonedDateTime(1));
            assertUnsupported(() -> reader.getDuration("v"));
            assertUnsupported(() -> reader.getDuration(1));
            assertUnsupported(() -> reader.getInet4Address("v"));
            assertUnsupported(() -> reader.getInet4Address(1));
            assertUnsupported(() -> reader.getInet6Address("v"));
            assertUnsupported(() -> reader.getInet6Address(1));
            assertUnsupported(() -> reader.getGeoPoint("v"));
            assertUnsupported(() -> reader.getGeoPoint(1));
            assertUnsupported(() -> reader.getGeoRing("v"));
            assertUnsupported(() -> reader.getGeoRing(1));
            assertUnsupported(() -> reader.getGeoPolygon("v"));
            assertUnsupported(() -> reader.getGeoPolygon(1));
            assertUnsupported(() -> reader.getGeoMultiPolygon("v"));
            assertUnsupported(() -> reader.getGeoMultiPolygon(1));
            assertUnsupported(() -> reader.getByteArray("v"));
            assertUnsupported(() -> reader.getByteArray(1));
            assertUnsupported(() -> reader.getIntArray("v"));
            assertUnsupported(() -> reader.getIntArray(1));
            assertUnsupported(() -> reader.getLongArray("v"));
            assertUnsupported(() -> reader.getLongArray(1));
            assertUnsupported(() -> reader.getFloatArray("v"));
            assertUnsupported(() -> reader.getFloatArray(1));
            assertUnsupported(() -> reader.getDoubleArray("v"));
            assertUnsupported(() -> reader.getDoubleArray(1));
            assertUnsupported(() -> reader.getBooleanArray("v"));
            assertUnsupported(() -> reader.getBooleanArray(1));
            assertUnsupported(() -> reader.getShortArray("v"));
            assertUnsupported(() -> reader.getShortArray(1));
            assertUnsupported(() -> reader.getStringArray("v"));
            assertUnsupported(() -> reader.getStringArray(1));
            assertUnsupported(() -> reader.getObjectArray("v"));
            assertUnsupported(() -> reader.getObjectArray(1));
            assertUnsupported(() -> reader.getClickHouseBitmap("v"));
            assertUnsupported(() -> reader.getClickHouseBitmap(1));
            assertUnsupported(() -> reader.getTemporalAmount("v"));
            assertUnsupported(() -> reader.getTemporalAmount(1));
        }
    }

    private static void assertUnsupported(Runnable r) {
        try {
            r.run();
            Assert.fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException expected) {
            // ok
        }
    }
}

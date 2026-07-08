package com.clickhouse.client.api.internal;

import com.clickhouse.data.ClickHouseColumn;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

@Test(groups = {"unit"})
public class DataTypeConverterTest {

    @Test
    public void testArrayToString() {
        DataTypeConverter converter = new DataTypeConverter();

        ClickHouseColumn intColumn = ClickHouseColumn.of("v", "Array(Int32)");
        assertEquals(converter.convertToString(new byte[]{1, 2, 3}, intColumn), "[1, 2, 3]");
        assertEquals(converter.convertToString(new short[]{1, 2, 3}, intColumn), "[1, 2, 3]");
        assertEquals(converter.convertToString(new int[]{1, 2, 3}, intColumn), "[1, 2, 3]");
        assertEquals(converter.convertToString(new long[]{1L, 2L, 3L}, intColumn), "[1, 2, 3]");
        assertEquals(converter.convertToString(new float[]{1.0f, 2.0f, 3.0f}, intColumn), "[1.0, 2.0, 3.0]");
        assertEquals(converter.convertToString(new double[]{1.0d, 2.0d, 3.0d}, intColumn), "[1.0, 2.0, 3.0]");
        assertEquals(converter.convertToString(new boolean[]{true, false, true}, intColumn), "[true, false, true]");


        ClickHouseColumn strColumn = ClickHouseColumn.of("v", "Array(String)");
        assertEquals(converter.convertToString(new String[][]{{"a", null}, {"b", "c"}}, strColumn), "[['a', NULL], ['b', 'c']]");
        assertEquals(converter.convertToString(new int[][]{{1, 2}, {3, 4}}, intColumn), "[[1, 2], [3, 4]]");
        assertEquals(converter.convertToString(new int[][][]{{{1, 2}, {3, 4}}, {{5, 6}}}, intColumn), "[[[1, 2], [3, 4]], [[5, 6]]]");
        assertEquals(converter.convertToString(new char[]{'a', 'b', 'c'}, strColumn), "['a', 'b', 'c']");
    }

    @Test
    public void testListToString() {
        DataTypeConverter converter = new DataTypeConverter();
        ClickHouseColumn column = ClickHouseColumn.of("field", "Array(Int32)");
        assertEquals(converter.convertToString(Collections.emptyList(), column), "[]");
        assertEquals(converter.convertToString(Arrays.asList(1, 2, 3), column), "[1, 2, 3]");
        assertEquals(converter.convertToString(Arrays.asList(1, null, 3), column), "[1, NULL, 3]");
        assertEquals(converter.convertToString(Arrays.asList(Arrays.asList(1, 2), Arrays.asList(3, 4)), column), "[[1, 2], [3, 4]]");
        assertEquals(converter.convertToString(Arrays.asList(Arrays.asList(Arrays.asList(1, 2), Arrays.asList(3, 4)), Arrays.asList(Arrays.asList(5, 6))), column), "[[[1, 2], [3, 4]], [[5, 6]]]");
        assertEquals(converter.convertToString(Arrays.asList(null, null, null), column), "[NULL, NULL, NULL]");
    }

    @Test
    public void testDateToString() {
        DataTypeConverter converter = new DataTypeConverter();
        ClickHouseColumn column = ClickHouseColumn.of("field", "Date");
        assertEquals(converter.convertToString(LocalDate.of(2022, 1, 1), column), "2022-01-01");
        assertEquals(converter.convertToString(LocalDate.of(2022, 1, 2), column), "2022-01-02");
        assertEquals(converter.convertToString(LocalDate.of(2022, 1, 3), column), "2022-01-03");

        Date date = Date.from(ZonedDateTime.of(2022, 1, 4, 12, 34, 56, 0, ZoneId.of("Asia/Shanghai")).toInstant());
        assertEquals(converter.convertToString(date, column), "2022-01-04");
        Date sqlDate = java.sql.Date.valueOf("2022-01-04");
        assertEquals(converter.convertToString(sqlDate, column), "2022-01-04");
        java.sql.Time sqlTime = java.sql.Time.valueOf("12:34:56");
        assertEquals(converter.convertToString(sqlTime, column), "1970-01-01");
    }


    @Test
    public void testTimeToString() {
        DataTypeConverter converter = new DataTypeConverter();
        ClickHouseColumn column = ClickHouseColumn.of("field", "Time");
        assertEquals(converter.timeToString(LocalTime.of(12, 34, 56), column), "12:34:56");
        assertEquals(converter.timeToString(LocalTime.of(23, 59, 59), column), "23:59:59");
        Date sqlDate = java.sql.Date.valueOf("2022-01-04");
        assertEquals(converter.convertToString(sqlDate, column), "00:00:00");
        Date date = Date.from(ZonedDateTime.of(2022, 1, 4, 12, 34, 56, 0, ZoneId.of("Asia/Shanghai")).toInstant());
        assertEquals(converter.convertToString(date, column), "04:34:56");
        java.sql.Time sqlTime = java.sql.Time.valueOf("12:34:56");// 12:34:56 in ms
        assertEquals(converter.convertToString(sqlTime, column), "12:34:56");
    }


    @Test
    public void testDateTimeToString() {
        DataTypeConverter converter = new DataTypeConverter();
        ClickHouseColumn column = ClickHouseColumn.of("field", "DateTime");
        assertEquals(converter.dateTimeToString(LocalDateTime.of(2022, 1, 1, 12, 34, 56), column), "2022-01-01 12:34:56");
        assertEquals(converter.dateTimeToString(LocalDateTime.of(2022, 1, 2, 23, 59, 59), column), "2022-01-02 23:59:59");

        Date date = Date.from(ZonedDateTime.of(2022, 1, 4, 12, 34, 56, 0, ZoneId.of("Asia/Shanghai")).toInstant());
        assertEquals(converter.convertToString(date, column), "2022-01-04 04:34:56");
        Date sqlDate = java.sql.Date.valueOf("2022-01-04");
        assertEquals(converter.convertToString(sqlDate, column), "1970-01-01 00:00:00");
        java.sql.Time sqlTime = java.sql.Time.valueOf("12:34:56");
        assertEquals(converter.convertToString(sqlTime, column), "1970-01-01 12:34:56");
    }

    @Test
    public void testEnumToString() {
        DataTypeConverter converter = new DataTypeConverter();
        ClickHouseColumn column = ClickHouseColumn.of("field", "Enum8('a' = 1, 'b' = 2)");
        assertEquals(converter.convertToString("a", column), "a");
        assertNull(converter.convertToString(null, column));
        assertEquals(converter.convertToString(1, column), "a");
        assertEquals(converter.convertToString("1234567", column), "1234567");

        column = ClickHouseColumn.of("field", "Enum8('a' = 1, 'b' = 2)");
        assertEquals(converter.convertToString("a", column), "a");
        assertNull(converter.convertToString(null, column));
        assertEquals(converter.convertToString(1, column), "a");
        assertEquals(converter.convertToString("1234567", column), "1234567");

        column = ClickHouseColumn.of("field", "Enum8('a' = 1, 'b' = 2)");
        assertEquals(converter.convertToString("a", column), "a");
        assertNull(converter.convertToString(null, column));
        assertEquals(converter.convertToString(2, column), "b");
        assertEquals(converter.convertToString("1234567", column), "1234567");

        column = ClickHouseColumn.of("field", "Variant(Enum8('a' = 1, 'b' = 2))");
        assertEquals(converter.convertToString("a", column), "a");
        assertEquals(converter.convertToString(null, column), null);
        assertEquals(converter.convertToString(1, column), "1");
        assertEquals(converter.convertToString("1234567", column), "1234567");
        assertEquals(converter.convertToString(2, column), "2");
    }

    @Test
    public void testGeoToString() {
        DataTypeConverter converter = new DataTypeConverter();

        assertEquals(converter.convertToString(new double[] {1D, 2D}, ClickHouseColumn.of("field", "Point")), "(1.0,2.0)");
        assertEquals(converter.convertToString(new double[][] {{1D, 2D}, {3D, 4D}}, ClickHouseColumn.of("field", "Ring")),
                "[(1.0,2.0),(3.0,4.0)]");
        assertEquals(converter.convertToString(new double[][][] {{{1D, 2D}, {3D, 4D}}}, ClickHouseColumn.of("field", "Polygon")),
                "[[(1.0,2.0),(3.0,4.0)]]");
        assertEquals(
                converter.convertToString(new double[][][][] {{{{1D, 2D}, {3D, 4D}}}}, ClickHouseColumn.of("field", "MultiPolygon")),
                "[[[(1.0,2.0),(3.0,4.0)]]]");
        assertEquals(converter.convertToString(new double[] {1D, 2D}, ClickHouseColumn.of("field", "Geometry")), "(1.0,2.0)");
    }

    @Test
    public void testVariantOrDynamicGeoToString() {
        DataTypeConverter converter = new DataTypeConverter();

        assertEquals(converter.convertToString(new double[] {1D, 2D}, ClickHouseColumn.of("field", "Variant(String, Point)")),
                "(1.0,2.0)");
        assertEquals(
                converter.convertToString(new double[][] {{1D, 2D}, {3D, 4D}}, ClickHouseColumn.of("field", "Dynamic")),
                "[(1.0,2.0),(3.0,4.0)]");
        assertEquals(
                converter.convertToString(new double[][][] {{{1D, 2D}, {3D, 4D}}}, ClickHouseColumn.of("field", "Variant(String, Polygon)")),
                "[[(1.0,2.0),(3.0,4.0)]]");
        assertEquals(
                converter.convertToString(new double[][][][] {{{{1D, 2D}, {3D, 4D}}}}, ClickHouseColumn.of("field", "Dynamic")),
                "[[[(1.0,2.0),(3.0,4.0)]]]");
        assertEquals(
                converter.convertToString(new int[] {1, 2, 3}, ClickHouseColumn.of("field", "Variant(String, Array(Int32))")),
                "[1, 2, 3]");
        assertEquals(
                converter.convertToString(new double[][] {{1D, 2D, 3D}}, ClickHouseColumn.of("field", "Dynamic")),
                "[[1.0, 2.0, 3.0]]");
    }

    @DataProvider(name = "queryParameters")
    public static Object[][] queryParameters() {
        return new Object[][] {
                // --- Scalars: bare, UNQUOTED text form. The server reads a scalar {name:Type} value
                // verbatim, so quoting it (e.g. '2026-05-13' for a Date) is rejected. ---
                {LocalDate.of(2026, 5, 13), "2026-05-13"},
                {"hello", "hello"},
                {42, "42"},
                {new BigDecimal("1.50"), "1.50"},
                {null, "null"},

                // --- Array/List with String/temporal leaves: single-quoted so the server's array
                // text parser accepts them (previously emitted e.g. [2026-05-13] -> HTTP 400). ---
                {Arrays.asList(LocalDate.of(2026, 5, 13), LocalDate.of(2026, 5, 14)), "['2026-05-13','2026-05-14']"},
                {Arrays.asList("a", "b"), "['a','b']"},
                {Collections.singletonList(LocalDateTime.of(2026, 5, 13, 16, 10, 0)), "['2026-05-13 16:10:00']"},
                {new LocalDate[] {LocalDate.of(2026, 5, 13)}, "['2026-05-13']"},

                // --- Primitive arrays are detected via getClass().isArray() and iterated reflectively
                // (Array.get autoboxes), so they are no longer mis-rendered as a scalar "[I@..". ---
                {new int[] {1, 2, 3}, "[1,2,3]"},
                {new double[] {1.0d, 2.5d}, "[1.0,2.5]"},
                {new boolean[] {true, false}, "[true,false]"},
                // byte[] has no declared type here, so it is treated as a numeric array (Array(Int8)).
                {new byte[] {1, 2, 3}, "[1,2,3]"},

                // --- Nested containers, including a nested primitive array (List<int[]>). ---
                {Collections.singletonList(Collections.singletonList(LocalDate.of(2026, 5, 13))), "[['2026-05-13']]"},
                {Collections.singletonList(new int[] {1, 2}), "[[1,2]]"},

                // --- Map: {k:v} (no spaces), keys/values formatted like array leaves. ---
                {Collections.singletonMap("k", LocalDate.of(2026, 5, 13)), "{'k':'2026-05-13'}"},

                // --- Escaping and null leaves. ---
                {Collections.singletonList("a'b"), "['a\\'b']"},
                {Arrays.asList(LocalDate.of(2026, 5, 13), null), "['2026-05-13',NULL]"},

                // --- Contrast: numeric containers must stay UNQUOTED (quoting an Array(Int32)/
                // Array(Decimal) element causes the server to reject it with CANNOT_READ_ARRAY_FROM_TEXT). ---
                {Arrays.asList(1, 2, 3), "[1,2,3]"},
                {Collections.singletonList(new BigDecimal("1.50")), "[1.50]"},

                // --- Boundary: empty containers. ---
                {Collections.emptyList(), "[]"},
                {Collections.emptyMap(), "{}"},
        };
    }

    @Test(dataProvider = "queryParameters")
    public void testConvertParameterToString(Object value, String expected) {
        assertEquals(new DataTypeConverter().convertParameterToString(value), expected);
    }

    @Test
    public void testConvertDeeplyNestedParameterDoesNotOverflow() {
        // convertParameterContainer uses an explicit work stack (not recursion), so a deeply nested
        // container must format correctly without a StackOverflowError. Recursion overflowed here.
        int depth = 20_000;
        Object nested = LocalDate.of(2026, 5, 13);
        for (int i = 0; i < depth; i++) {
            nested = Collections.singletonList(nested);
        }

        StringBuilder expected = new StringBuilder(depth * 2 + 12);
        for (int i = 0; i < depth; i++) {
            expected.append('[');
        }
        expected.append("'2026-05-13'");
        for (int i = 0; i < depth; i++) {
            expected.append(']');
        }

        assertEquals(new DataTypeConverter().convertParameterToString(nested), expected.toString());
    }
}
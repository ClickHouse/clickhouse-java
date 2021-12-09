package ru.yandex.clickhouse.response.parser;

import java.sql.SQLException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import ru.yandex.clickhouse.response.ByteFragment;
import ru.yandex.clickhouse.response.ClickHouseColumnInfo;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class ClickHouseValueParserTest {
    /**
     * Generates test data for floats.
     */
    @DataProvider(name = "float_test_data")
    public Object[][] floatTestData() {
        return new Object [][] {
                {"100.0", 100.0f},
                {"NaN", Float.NaN},
                {"Infinity", Float.POSITIVE_INFINITY},
                {"+Infinity", Float.POSITIVE_INFINITY},
                {"-Infinity", Float.NEGATIVE_INFINITY},
                {"nan", Float.NaN},
                {"inf", Float.POSITIVE_INFINITY},
                {"+inf", Float.POSITIVE_INFINITY},
                {"-inf", Float.NEGATIVE_INFINITY}
        };
    }

    /**
     * Generates test data for doubles.
     */
    @DataProvider(name = "double_test_data")
    public Object[][] doubleTestData() {
        return new Object [][] {
                {"100.0", 100.0d},
                {"Infinity", Double.POSITIVE_INFINITY},
                {"+Infinity", Double.POSITIVE_INFINITY},
                {"-Infinity", Double.NEGATIVE_INFINITY},
                {"nan", Double.NaN},
                {"inf", Double.POSITIVE_INFINITY},
                {"+inf", Double.POSITIVE_INFINITY},
                {"-inf", Double.NEGATIVE_INFINITY}
        };
    }

    @Test(groups = "unit")
    public void testParseInt() throws Exception {
        ClickHouseColumnInfo columnInfo = ClickHouseColumnInfo.parse("Int64", "columnName", null);
        assertEquals(ClickHouseValueParser.parseInt(ByteFragment.fromString("42"), columnInfo), 42);
        assertEquals(ClickHouseValueParser.parseInt(ByteFragment.fromString("-42"), columnInfo), -42);
        assertEquals(ClickHouseValueParser.parseInt(ByteFragment.fromString(""), columnInfo), 0);
        assertEquals(0, ClickHouseValueParser.parseInt(ByteFragment.fromString("0"), columnInfo), 0);
        assertEquals(0, ClickHouseValueParser.parseInt(ByteFragment.fromString("000"), columnInfo), 0);
        assertEquals(0, ClickHouseValueParser.parseInt(ByteFragment.fromString("\\N"), columnInfo), 0 );
        try {
            ClickHouseValueParser.parseInt(ByteFragment.fromString("foo"), columnInfo);
            fail();
        } catch (SQLException sqle) {
            // expected
        }
        assertEquals(
            ClickHouseValueParser.parseInt(
                ByteFragment.fromString(String.valueOf(Integer.MAX_VALUE)),
                columnInfo),
            Integer.MAX_VALUE);
        try {
            ClickHouseValueParser.parseInt(
                ByteFragment.fromString(Long.valueOf(Integer.MAX_VALUE + 1L).toString()),
                columnInfo);
            fail();
        } catch (SQLException sqle) {
            // expected
        }
        assertEquals(
            ClickHouseValueParser.parseInt(
                ByteFragment.fromString(String.valueOf(Integer.MIN_VALUE)),
                columnInfo),
            Integer.MIN_VALUE);
        try {
            ClickHouseValueParser.parseInt(
                ByteFragment.fromString(Long.valueOf(Integer.MIN_VALUE - 1L).toString()),
                columnInfo);
            fail();
        } catch (SQLException sqle) {
            // expected
        }
    }

    @Test(groups = "unit")
    public void testParseLong() throws Exception {
        ClickHouseColumnInfo columnInfo = ClickHouseColumnInfo.parse("Int64", "columnName", null);
        assertEquals(ClickHouseValueParser.parseLong(ByteFragment.fromString("42"), columnInfo), 42);
        assertEquals(ClickHouseValueParser.parseLong(ByteFragment.fromString("-42"), columnInfo), -42);
        assertEquals(ClickHouseValueParser.parseLong(ByteFragment.fromString(""), columnInfo), 0L);
        assertEquals(ClickHouseValueParser.parseLong(ByteFragment.fromString("0"), columnInfo), 0L);
        assertEquals(ClickHouseValueParser.parseLong(ByteFragment.fromString("000"), columnInfo), 0L);
        assertEquals(ClickHouseValueParser.parseLong(ByteFragment.fromString("\\N"), columnInfo), 0L);
        try {
            ClickHouseValueParser.parseLong(ByteFragment.fromString("foo"), columnInfo);
            fail();
        } catch (SQLException sqle) {
            // expected
        }
        assertEquals(
            ClickHouseValueParser.parseLong(
                ByteFragment.fromString(String.valueOf(Long.MAX_VALUE)),
                columnInfo),
            Long.MAX_VALUE);
        try {
            ClickHouseValueParser.parseLong(
                ByteFragment.fromString(
                    Long.valueOf(Long.MAX_VALUE).toString() + "0"),
                columnInfo);
            fail();
        } catch (SQLException sqle) {
            // expected
        }
        assertEquals(
            ClickHouseValueParser.parseLong(
                ByteFragment.fromString(String.valueOf(Long.MIN_VALUE)),
                columnInfo),
            Long.MIN_VALUE);
        try {
            ClickHouseValueParser.parseLong(
                ByteFragment.fromString(Long.valueOf(Long.MIN_VALUE).toString() + "0"),
                columnInfo);
            fail();
        } catch (SQLException sqle) {
            // expected
        }
    }

    @Test(groups = "unit")
    public void testParseShort() throws Exception {
        ClickHouseColumnInfo columnInfo = ClickHouseColumnInfo.parse("UInt16", "columnName", null);
        assertEquals(ClickHouseValueParser.parseShort(ByteFragment.fromString("42"), columnInfo), 42);
        assertEquals(ClickHouseValueParser.parseShort(ByteFragment.fromString("-42"), columnInfo), -42);
        assertEquals(ClickHouseValueParser.parseShort(ByteFragment.fromString(""), columnInfo), 0);
        assertEquals(ClickHouseValueParser.parseShort(ByteFragment.fromString("0"), columnInfo), 0);
        assertEquals(ClickHouseValueParser.parseShort(ByteFragment.fromString("000"), columnInfo), 0);
        assertEquals(ClickHouseValueParser.parseShort(ByteFragment.fromString("\\N"), columnInfo), 0);
        try {
            ClickHouseValueParser.parseShort(ByteFragment.fromString("foo"), columnInfo);
            fail();
        } catch (SQLException sqle) {
            // expected
        }
        assertEquals(
            ClickHouseValueParser.parseShort(
                ByteFragment.fromString(String.valueOf(Short.MAX_VALUE)),
                columnInfo),
            Short.MAX_VALUE);
        try {
            ClickHouseValueParser.parseShort(
                ByteFragment.fromString(Integer.valueOf(Short.MAX_VALUE + 1).toString()),
                columnInfo);
            fail();
        } catch (SQLException sqle) {
            // expected
        }
        assertEquals(
            ClickHouseValueParser.parseShort(
                ByteFragment.fromString(String.valueOf(Short.MIN_VALUE)),
                columnInfo),
            Short.MIN_VALUE);
        try {
            ClickHouseValueParser.parseShort(
                ByteFragment.fromString(Integer.valueOf(Short.MIN_VALUE - 1).toString()),
                columnInfo);
            fail();
        } catch (SQLException sqle) {
            // expected
        }
    }

    @Test(groups = "unit")
    public void testParseBoolean() throws SQLException {
        ClickHouseColumnInfo columnInfo = ClickHouseColumnInfo.parse("UInt8", "columnName", null);
        assertFalse(ClickHouseValueParser.parseBoolean(ByteFragment.fromString(""), columnInfo));
        assertFalse(ClickHouseValueParser.parseBoolean(ByteFragment.fromString("\\N"), columnInfo));
        assertFalse(ClickHouseValueParser.parseBoolean(ByteFragment.fromString("0"), columnInfo));
        assertTrue(ClickHouseValueParser.parseBoolean(ByteFragment.fromString("1"), columnInfo));
        assertFalse(ClickHouseValueParser.parseBoolean(ByteFragment.fromString("10"), columnInfo));
        assertFalse(ClickHouseValueParser.parseBoolean(ByteFragment.fromString(" 10"), columnInfo));
        assertFalse(ClickHouseValueParser.parseBoolean(ByteFragment.fromString("foo"), columnInfo));
        assertTrue(ClickHouseValueParser.parseBoolean(ByteFragment.fromString("TRUE"), columnInfo));
        assertTrue(ClickHouseValueParser.parseBoolean(ByteFragment.fromString("true"), columnInfo));
        assertFalse(ClickHouseValueParser.parseBoolean(ByteFragment.fromString("FALSE"), columnInfo));
        assertFalse(ClickHouseValueParser.parseBoolean(ByteFragment.fromString("false"), columnInfo));
        assertFalse(ClickHouseValueParser.parseBoolean(ByteFragment.fromString(" true"), columnInfo));
    }

    @Test (dataProvider = "float_test_data")
    public void testParseFloat(String byteFragmentString, Float expectedValue) throws SQLException {
        ClickHouseColumnInfo columnInfo = ClickHouseColumnInfo.parse("Float32", "columnName", null);
        float floatDelta = 0.001f;
        if (expectedValue.isNaN()) {
            assertTrue(Float.isNaN(ClickHouseValueParser.parseFloat(
                    ByteFragment.fromString(byteFragmentString), columnInfo)
            ));
        } else {
            assertEquals(ClickHouseValueParser.parseFloat(
                    ByteFragment.fromString(byteFragmentString), columnInfo), expectedValue, floatDelta
            );
        }
    }

    @Test (dataProvider = "double_test_data")
    public void testParseDouble(String byteFragmentString, Double expectedValue) throws SQLException {
        ClickHouseColumnInfo columnInfo = ClickHouseColumnInfo.parse("Float64", "columnName", null);
        double doubleDelta = 0.001;
        if (expectedValue.isNaN()) {
            assertTrue(Double.isNaN(ClickHouseValueParser.parseDouble(
                    ByteFragment.fromString(byteFragmentString), columnInfo)
            ));
        } else {
            assertEquals(ClickHouseValueParser.parseDouble(
                    ByteFragment.fromString(byteFragmentString), columnInfo), expectedValue, doubleDelta
            );
        }
    }

    @Test (dataProvider = "float_test_data")
    public void testGetParserFloat(String byteFragmentString, Float expectedValue) throws SQLException {
        ClickHouseColumnInfo columnInfo = ClickHouseColumnInfo.parse("Float32", "columnName", null);
        float floatDelta = 0.001f;

        if (expectedValue.isNaN()) {
            assertTrue(Float.isNaN(ClickHouseValueParser.getParser(Float.class).parse(
                    ByteFragment.fromString(byteFragmentString), columnInfo, null)
            ));
        } else {
            assertEquals(ClickHouseValueParser.getParser(Float.class).parse(
                    ByteFragment.fromString(byteFragmentString), columnInfo, null), expectedValue, floatDelta
            );
        }
    }

    @Test (dataProvider = "double_test_data")
    public void testGetParserDouble(String byteFragmentString, Double expectedValue) throws SQLException {
        ClickHouseColumnInfo columnInfo = ClickHouseColumnInfo.parse("Float64", "columnName", null);
        double doubleDelta = 0.001d;

        if (expectedValue.isNaN()) {
            assertTrue(Double.isNaN(ClickHouseValueParser.getParser(Double.class).parse(
                    ByteFragment.fromString(byteFragmentString), columnInfo, null)
            ));
        } else {
            assertEquals(ClickHouseValueParser.getParser(Double.class).parse(
                    ByteFragment.fromString(byteFragmentString), columnInfo, null), expectedValue, doubleDelta
            );
        }
    }
}

package ru.yandex.clickhouse.response.parser;

import java.sql.SQLException;

import org.testng.annotations.Test;

import ru.yandex.clickhouse.response.ByteFragment;
import ru.yandex.clickhouse.response.ClickHouseColumnInfo;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class ClickHouseValueParserTest {

    @Test
    public void testParseInt() throws Exception {
        ClickHouseColumnInfo columnInfo = ClickHouseColumnInfo.parse("Int64", "columnName");
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

    @Test
    public void testParseLong() throws Exception {
        ClickHouseColumnInfo columnInfo = ClickHouseColumnInfo.parse("Int64", "columnName");
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

    @Test
    public void testParseShort() throws Exception {
        ClickHouseColumnInfo columnInfo = ClickHouseColumnInfo.parse("UInt16", "columnName");
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

    @Test
    public void testParseBoolean() throws SQLException {
        ClickHouseColumnInfo columnInfo = ClickHouseColumnInfo.parse("UInt8", "columnName");
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

}

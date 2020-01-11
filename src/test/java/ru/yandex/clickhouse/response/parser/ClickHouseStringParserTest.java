package ru.yandex.clickhouse.response.parser;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import ru.yandex.clickhouse.response.ByteFragment;
import ru.yandex.clickhouse.response.ClickHouseColumnInfo;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class ClickHouseStringParserTest {

    private ClickHouseStringParser parser;
    private ClickHouseColumnInfo columnInfo;

    @BeforeClass
    public void setUp() {
        parser = ClickHouseStringParser.getInstance();
        columnInfo = ClickHouseColumnInfo.parse("String", "column_name");
    }

    @Test
    public void testParseString() throws Exception {
        assertNull(parser.parse(ByteFragment.fromString("\\N"), columnInfo, null));
        assertEquals(parser.parse(ByteFragment.fromString(""), columnInfo, null), "");
        assertEquals(parser.parse(ByteFragment.fromString("null"), columnInfo, null), "null");
        assertEquals(parser.parse(ByteFragment.fromString("NULL"), columnInfo, null), "NULL");
        assertEquals(parser.parse(ByteFragment.fromString("foo"), columnInfo, null), "foo");
        assertEquals(
            parser.parse(ByteFragment.fromString(" \t \r\n"), columnInfo, null),
            " \t \r\n");
    }

}

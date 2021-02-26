package ru.yandex.clickhouse.response.parser;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import ru.yandex.clickhouse.response.ByteFragment;
import ru.yandex.clickhouse.response.ClickHouseColumnInfo;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class ClickHouseDoubleParserTest {

    private ClickHouseDoubleParser parser;
    private ClickHouseColumnInfo columnInfo;

    @BeforeClass
    public void setUp() {
        parser = ClickHouseDoubleParser.getInstance();
        columnInfo = ClickHouseColumnInfo.parse("Float64", "columnName");
    }

    @Test
    public void testParseDouble() throws Exception {
        assertNull(parse("\\N"));
        assertEquals(parse("0"), Double.valueOf(0.0));
        assertEquals(parse("-1.23"), Double.valueOf(-1.23));
        assertEquals(parse("1.23"), Double.valueOf(1.23));
        assertEquals(parse("nan"), Double.valueOf(Double.NaN));
        assertEquals(parse("NaN"), Double.valueOf(Double.NaN));
        assertEquals(parse("inf"), Double.valueOf(Double.POSITIVE_INFINITY));
        assertEquals(parse("+inf"), Double.valueOf(Double.POSITIVE_INFINITY));
        assertEquals(parse("-inf"), Double.valueOf(Double.NEGATIVE_INFINITY));
    }

    @Test
    public void testParseDefault() throws Exception {
        assertEquals(parseWithDefault("\\N"), Double.valueOf(0));
        assertEquals(parseWithDefault("nan"), Double.valueOf(Double.NaN));
    }

    private Double parse(String s) throws Exception {
        return parser.parse(ByteFragment.fromString(s), columnInfo, null);
    }

    private Double parseWithDefault(String s) throws Exception {
        return parser.parseWithDefault(ByteFragment.fromString(s), columnInfo, null);
    }

}

package ru.yandex.clickhouse.response.parser;

import java.math.BigDecimal;
import java.sql.SQLException;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import ru.yandex.clickhouse.response.ByteFragment;
import ru.yandex.clickhouse.response.ClickHouseColumnInfo;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

public class ClickHouseBigDecimalParserTest {

    private ClickHouseValueParser<BigDecimal> parser;
    private ClickHouseColumnInfo columnInfo;

    @BeforeClass
    public void setUp() throws Exception {
        parser = ClickHouseValueParser.getParser(BigDecimal.class);
        columnInfo = ClickHouseColumnInfo.parse("Float64", "column_name");
    }

    @Test
    public void testParseBigDecimal() throws Exception {
        assertNull(parser.parse(ByteFragment.fromString("\\N"), columnInfo, null));
        assertNull(parser.parse(ByteFragment.fromString(""), columnInfo, null));
        assertEquals(
            parser.parse(ByteFragment.fromString("0"), columnInfo, null),
            BigDecimal.ZERO);
        assertNotEquals(
            parser.parse(ByteFragment.fromString("0.000"), columnInfo, null),
            BigDecimal.ZERO);
        assertEquals(
            parser.parse(ByteFragment.fromString("0.000"), columnInfo, null),
            BigDecimal.ZERO.setScale(3));
        assertEquals(
            parser.parse(ByteFragment.fromString("42.23"), columnInfo, null),
            BigDecimal.valueOf(42.23));
        assertNotEquals(
            parser.parse(ByteFragment.fromString("42.230"), columnInfo, null),
            BigDecimal.valueOf(42.23));
        assertEquals(
            parser.parse(ByteFragment.fromString("-42.23"), columnInfo, null),
            BigDecimal.valueOf(-42.23));
        try {
            parser.parse(ByteFragment.fromString("foo"), columnInfo, null);
            fail();
        } catch (SQLException sqle) {
            // expected
        }
    }

}

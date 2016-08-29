package ru.yandex.clickhouse;


import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;


public class ClickHouseUtilTest {

    @Test
    public void testQuote() throws Exception {
        assertEquals("NULL", ClickHouseUtil.escape(null));
        assertEquals("test", ClickHouseUtil.escape("test"));
        assertEquals("t\\n\\0\\r\\test\\'", ClickHouseUtil.escape("t\n\0\r\test'"));
    }

    @Test
    public void testQuoteIdentifier() throws Exception {
        assertEquals("`z`", ClickHouseUtil.quoteIdentifier("z"));
        assertEquals("`a\\`\\' `", ClickHouseUtil.quoteIdentifier("a`' "));

        try {
            ClickHouseUtil.quoteIdentifier(null);
            fail("quiteIdentifier with null argument must fail");
        } catch (IllegalArgumentException ex) {
            // pass, it's ok
        }
    }
}
package ru.yandex.clickhouse;


import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

/**
 * Created by zhur on 01/03/16.
 */
public class ClickHouseUtilTest {

    @Test
    public void testQuote() throws Exception {
        assertEquals("NULL", ClickHouseUtil.quote(null));
        assertEquals("'test'", ClickHouseUtil.quote("test"));
        assertEquals("'t\\n\\0\\r\\test'", ClickHouseUtil.quote("t\n\0\r\test"));
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
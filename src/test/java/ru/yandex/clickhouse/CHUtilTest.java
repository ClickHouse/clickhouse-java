package ru.yandex.clickhouse;


import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

/**
 * Created by zhur on 01/03/16.
 */
public class CHUtilTest {

    @Test
    public void testQuote() throws Exception {
        assertEquals("NULL", CHUtil.quote(null));
        assertEquals("'test'", CHUtil.quote("test"));
        assertEquals("'t\\n\\0\\r\\test'", CHUtil.quote("t\n\0\r\test"));
    }

    @Test
    public void testQuoteIdentifier() throws Exception {
        assertEquals("`z`", CHUtil.quoteIdentifier("z"));
        assertEquals("`a\\`\\' `", CHUtil.quoteIdentifier("a`' "));

        try {
            CHUtil.quoteIdentifier(null);
            fail("quiteIdentifier with null argument must fail");
        } catch (IllegalArgumentException ex) {
            // pass, it's ok
        }
    }
}
package com.clickhouse.jdbc.parser;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHouseSqlUtilsTest {
    @Test(groups = "unit")
    public void testIsQuote() {
        Assert.assertFalse(ClickHouseSqlUtils.isQuote('\0'));

        Assert.assertTrue(ClickHouseSqlUtils.isQuote('"'));
        Assert.assertTrue(ClickHouseSqlUtils.isQuote('\''));
        Assert.assertTrue(ClickHouseSqlUtils.isQuote('`'));
    }

    @Test(groups = "unit")
    public void testEscape() {
        char[] quotes = new char[] { '"', '\'', '`' };
        String str;
        for (int i = 0; i < quotes.length; i++) {
            char quote = quotes[i];
            Assert.assertEquals(ClickHouseSqlUtils.escape(str = null, quote), str);
            Assert.assertEquals(ClickHouseSqlUtils.escape(str = "", quote),
                    String.valueOf(quote) + String.valueOf(quote));
            Assert.assertEquals(ClickHouseSqlUtils.escape(str = "\\any \\string\\", quote),
                    String.valueOf(quote) + "\\\\any \\\\string\\\\" + String.valueOf(quote));
            Assert.assertEquals(
                    ClickHouseSqlUtils.escape(str = String.valueOf(quote) + "any " + String.valueOf(quote) + "string",
                            quote),
                    String.valueOf(quote) + "\\" + String.valueOf(quote) + "any \\" + String.valueOf(quote) + "string"
                            + String.valueOf(quote));
            Assert.assertEquals(ClickHouseSqlUtils.escape(str = "\\any \\string\\" + String.valueOf(quote), quote),
                    String.valueOf(quote) + "\\\\any \\\\string\\\\\\" + String.valueOf(quote) + String.valueOf(quote));
            Assert.assertEquals(
                    ClickHouseSqlUtils.escape(str = String.valueOf(quote) + "\\any \\" + String.valueOf(quote)
                            + "string\\" + String.valueOf(quote), quote),
                    String.valueOf(quote) + "\\" + String.valueOf(quote) + "\\\\any \\\\\\" + String.valueOf(quote)
                            + "string" + "\\\\\\" + String.valueOf(quote) + String.valueOf(quote));
        }
    }

    @Test(groups = "unit")
    public void testUnescape() {
        String str;
        Assert.assertEquals(ClickHouseSqlUtils.unescape(str = null), str);
        Assert.assertEquals(ClickHouseSqlUtils.unescape(str = ""), str);
        Assert.assertEquals(ClickHouseSqlUtils.unescape(str = "\\any \\string\\"), str);
        char[] quotes = new char[] { '"', '\'', '`' };
        for (int i = 0; i < quotes.length; i++) {
            char quote = quotes[i];
            Assert.assertEquals(ClickHouseSqlUtils.unescape(str = String.valueOf(quote) + "1" + String.valueOf(quote)),
                    "1");
            Assert.assertEquals(ClickHouseSqlUtils.unescape(str = String.valueOf(quote) + "\\any \\string\\"), str);
            Assert.assertEquals(ClickHouseSqlUtils.unescape(str = "\\any \\string\\" + String.valueOf(quote)), str);
            Assert.assertEquals(
                    ClickHouseSqlUtils.unescape(str = String.valueOf(quote) + "\\any" + String.valueOf(quote)
                            + String.valueOf(quote) + "\\string\\" + String.valueOf(quote)),
                    "any" + String.valueOf(quote) + "string\\");
            Assert.assertEquals(
                    ClickHouseSqlUtils.unescape(str = String.valueOf(quote) + String.valueOf(quote) + "\\"
                            + String.valueOf(quote) + "any" + String.valueOf(quote) + String.valueOf(quote)
                            + "\\string\\" + String.valueOf(quote)),
                    String.valueOf(quote) + String.valueOf(quote) + "any" + String.valueOf(quote) + "string\\");
        }
    }
}

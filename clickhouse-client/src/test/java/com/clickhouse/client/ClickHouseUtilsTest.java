package com.clickhouse.client;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHouseUtilsTest {
    @Test(groups = { "unit" })
    public void testEscape() {
        Assert.assertEquals(ClickHouseUtils.escape(null, '\0'), null);
        Assert.assertEquals(ClickHouseUtils.escape("", '\''), "");
        Assert.assertEquals(ClickHouseUtils.escape("'", '\''), "\\'");
        Assert.assertEquals(ClickHouseUtils.escape("\\", '\0'), "\\\\");
        Assert.assertEquals(ClickHouseUtils.escape("\\'", '\0'), "\\\\'");
        Assert.assertEquals(ClickHouseUtils.escape("\\'", '\''), "\\\\\\'");
    }

    @Test(groups = { "unit" })
    public void testGetService() {
        ClickHouseClient client = null;
        try {
            client = ClickHouseUtils.getService(ClickHouseClient.class, null);
        } catch (Exception e) {
            Assert.fail("Should not fail");
        }

        Assert.assertNotNull(client);

        ClickHouseUtilsTest me = null;
        try {
            me = ClickHouseUtils.getService(ClickHouseUtilsTest.class, null);
            Assert.fail("Should fail");
        } catch (Exception e) {
            Assert.assertNotNull(e);
        }
        Assert.assertNull(me);

        try {
            me = ClickHouseUtils.getService(ClickHouseUtilsTest.class, new ClickHouseUtilsTest());
        } catch (Exception e) {
            Assert.fail("Should not fail");
        }
        Assert.assertNotNull(me);

        me = null;
        try {
            me = ClickHouseUtils.getService(ClickHouseUtilsTest.class, () -> new ClickHouseUtilsTest());
        } catch (Exception e) {
            Assert.fail("Should not fail");
        }
        Assert.assertNotNull(me);
    }

    @Test(groups = { "unit" })
    public void testSkipBrackets() {
        final String args = "select * except(`a({[]})a`('bbb')[1]) from table";
        Assert.assertEquals(ClickHouseUtils.skipBrackets(args, args.indexOf('('), args.length(), '('),
                args.lastIndexOf(')') + 1);
        Assert.assertEquals(ClickHouseUtils.skipBrackets(args, args.indexOf('('), args.length(), '('),
                args.lastIndexOf(')') + 1);
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ClickHouseUtils.skipBrackets(args, 0, args.length(), '('));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ClickHouseUtils.skipBrackets(args, 0, args.length(), '['));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ClickHouseUtils.skipBrackets(args, 0, args.length(), '{'));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ClickHouseUtils.skipBrackets(args, 0, args.length(), '/'));

        String newArgs = ")";
        Assert.assertEquals(ClickHouseUtils.skipBrackets(newArgs, 0, newArgs.length(), '('), newArgs.length());
        Assert.assertEquals(ClickHouseUtils.skipBrackets(newArgs = "))", 0, newArgs.length(), '('),
                newArgs.lastIndexOf(')'));
        Assert.assertEquals(ClickHouseUtils.skipBrackets(newArgs = ")]", 0, newArgs.length(), '['), newArgs.length());
        Assert.assertEquals(ClickHouseUtils.skipBrackets(newArgs = "{}", 0, newArgs.length(), '{'), newArgs.length());
        Assert.assertEquals(ClickHouseUtils.skipBrackets(newArgs = "{ '''\\'}'}", 0, newArgs.length(), '{'),
                newArgs.length());
        Assert.assertEquals(ClickHouseUtils.skipBrackets(newArgs = "{ -- }\n/*/*}*/*/}", 0, newArgs.length(), '{'),
                newArgs.length());
    }

    @Test(groups = { "unit" })
    public void testSkipQuotedString() {
        final String args = "1'2'";
        Assert.assertEquals(ClickHouseUtils.skipQuotedString(args, 0, args.length(), '\''), 2);
        Assert.assertEquals(ClickHouseUtils.skipQuotedString(args, 2, args.length(), '\''), args.length());
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ClickHouseUtils.skipQuotedString(args, 0, args.length(), '`'));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ClickHouseUtils.skipQuotedString(args, 0, args.length(), '"'));
        Assert.assertEquals(ClickHouseUtils.skipQuotedString(args, 2, args.length(), '\''), args.length());

        String newArgs = "''''";
        Assert.assertEquals(ClickHouseUtils.skipQuotedString(newArgs, 0, newArgs.length(), '\''), newArgs.length());
        Assert.assertEquals(ClickHouseUtils.skipQuotedString(newArgs = "''''''", 0, newArgs.length(), '\''),
                newArgs.length());
        Assert.assertEquals(ClickHouseUtils.skipQuotedString(newArgs = "'\\''''", 0, newArgs.length(), '\''),
                newArgs.length());
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseUtils.skipQuotedString("", 0, 0, '\''));
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseUtils.skipQuotedString("'", 0, 1, '\''));
        Assert.assertEquals(ClickHouseUtils.skipQuotedString(newArgs = "''", 0, newArgs.length(), '\''),
                newArgs.length());
        Assert.assertEquals(ClickHouseUtils.skipQuotedString(newArgs = " '''\\'}'", 0, newArgs.length(), '\''),
                newArgs.indexOf('\\'));
        Assert.assertEquals(ClickHouseUtils.skipQuotedString(newArgs, 1, newArgs.length(), '\''), newArgs.length());
        Assert.assertEquals(ClickHouseUtils.skipQuotedString(newArgs, 1, newArgs.length(), '\''), newArgs.length());
        Assert.assertEquals(ClickHouseUtils.skipQuotedString(newArgs = "'\\\\'''", 0, newArgs.length(), '\''),
                newArgs.length());
    }

    @Test(groups = { "unit" })
    public void testSkipSingleLineComment() {
        String args = "select 1 -- select one\n union all select 2 -- select two--";
        Assert.assertEquals(ClickHouseUtils.skipSingleLineComment(args, 11, args.length()), args.indexOf('\n') + 1);
        Assert.assertEquals(ClickHouseUtils.skipSingleLineComment(args, args.indexOf("--", 11), args.length()),
                args.length());
    }

    @Test(groups = { "unit" })
    public void testSkipMultipleLineComment() {
        String args = "select 1 /* select 1/*one*/ -- a */, 2";
        Assert.assertEquals(ClickHouseUtils.skipMultiLineComment(args, 11, args.length()), args.lastIndexOf("*/") + 2);
        Assert.assertEquals(ClickHouseUtils.skipMultiLineComment(args, 21, args.length()), args.indexOf("*/", 21) + 2);
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ClickHouseUtils.skipMultiLineComment(args, args.lastIndexOf("*/") + 1, args.length()));
    }

    @Test(groups = { "unit" })
    public void testSkipContentsUntil() {
        String args = "select 'a' as `--b`,1/*('1(/*'*/(*/ from number(10)";
        Assert.assertEquals(ClickHouseUtils.skipContentsUntil(args, 0, args.length(), '('), args.lastIndexOf('(') + 1);

        args = "column1 AggregateFunction(quantiles(0.5, 0.9), UInt64),\ncolumn2 UInt8 not null";
        Assert.assertEquals(ClickHouseUtils.skipContentsUntil(args, 0, args.length(), ','), args.lastIndexOf(',') + 1);
        Assert.assertEquals(ClickHouseUtils.skipContentsUntil(args, 0, args.length(), '@'), args.length());
    }

    @Test(groups = { "unit" })
    public void testReadNameOrQuotedString() {
        String args = "123";
        StringBuilder builder = new StringBuilder();
        Assert.assertEquals(ClickHouseUtils.readNameOrQuotedString(args, 0, args.length(), builder), args.length());
        Assert.assertEquals(builder.toString(), args);

        builder.setLength(0);
        Assert.assertEquals(ClickHouseUtils.readNameOrQuotedString(args = " 123", 1, args.length(), builder),
                args.length());
        Assert.assertEquals(builder.toString(), "123");

        builder.setLength(0);
        Assert.assertEquals(ClickHouseUtils.readNameOrQuotedString(args = " `1\"'2``3` ", 1, args.length(), builder),
                args.lastIndexOf('`') + 1);
        Assert.assertEquals(builder.toString(), "1\"'2`3");
    }

    @Test(groups = { "unit" })
    public void testReadEnumValues() {
        String args = "Enum( ' `''1\" ' = 1, '\\''=2 )";
        Map<String, Integer> values = new HashMap<>();
        Assert.assertEquals(ClickHouseUtils.readEnumValues(args, 5, args.length(), values), args.lastIndexOf(')') + 1);
        Assert.assertEquals(values.size(), 2);
        Assert.assertEquals(values.get(" `'1\" "), (Integer) 1);
        Assert.assertEquals(values.get("'"), (Integer) 2);

        values.clear();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            String columns = "Enum () ";
            ClickHouseUtils.readEnumValues(columns, 6, columns.length(), values);
        });

        values.clear();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            String columns = "Enum ( 1 = '3'     )";
            ClickHouseUtils.readEnumValues(columns, 6, columns.length(), values);
        });

        args = "a Enum('1)'=2), b UInt8";
        values.clear();
        Assert.assertEquals(ClickHouseUtils.readEnumValues(args, 7, args.length(), values), args.lastIndexOf(','));
        Assert.assertEquals(values.size(), 1);
        Assert.assertEquals(values.get("1)"), (Integer) 2);
    }

    @Test(groups = { "unit" })
    public void testReadParameters() {
        String args = "column1 AggregateFunction( quantiles(0.5, 'c \\'''([1],2) d',0.9) , UInt64),\ncolumn2 UInt8 not null";
        List<String> params = new LinkedList<>();
        Assert.assertEquals(ClickHouseUtils.readParameters(args, args.indexOf('('), args.length(), params),
                args.lastIndexOf(')') + 1);
        Assert.assertEquals(params.size(), 2);
    }

    @Test(groups = { "unit" })
    public void testFileInputStream() {
        Assert.assertThrows(FileNotFoundException.class,
                () -> ClickHouseUtils.getFileInputStream(UUID.randomUUID().toString()));
    }
}

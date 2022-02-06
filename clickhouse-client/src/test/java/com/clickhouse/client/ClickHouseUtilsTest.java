package com.clickhouse.client;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Collections;
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
    public void testGetKeyValuePair() {
        Assert.assertEquals(ClickHouseUtils.getKeyValuePairs(null), Collections.emptyMap());
        Assert.assertEquals(ClickHouseUtils.getKeyValuePairs(""), Collections.emptyMap());
        Assert.assertEquals(ClickHouseUtils.getKeyValuePairs(" "), Collections.emptyMap());
        Assert.assertEquals(ClickHouseUtils.getKeyValuePairs("="), Collections.emptyMap());
        Assert.assertEquals(ClickHouseUtils.getKeyValuePairs("=="), Collections.emptyMap());
        Assert.assertEquals(ClickHouseUtils.getKeyValuePairs(","), Collections.emptyMap());
        Assert.assertEquals(ClickHouseUtils.getKeyValuePairs(",,"), Collections.emptyMap());
        Assert.assertEquals(ClickHouseUtils.getKeyValuePairs("=,"), Collections.emptyMap());
        Assert.assertEquals(ClickHouseUtils.getKeyValuePairs(" =\r ,"), Collections.emptyMap());
        Assert.assertEquals(ClickHouseUtils.getKeyValuePairs("a\\=='b c',"),
                Collections.singletonMap("a=", "'b c'"));
        Assert.assertEquals(ClickHouseUtils.getKeyValuePairs("User-Agent=New Client, X-Forward-For=1\\,2"),
                new HashMap<String, String>() {
                    {
                        put("User-Agent", "New Client");
                        put("X-Forward-For", "1,2");
                    }
                });
    }

    @Test(groups = { "unit" })
    public void testGetLeadingComment() {
        Assert.assertEquals(ClickHouseUtils.getLeadingComment(null), "");
        Assert.assertEquals(ClickHouseUtils.getLeadingComment(""), "");
        Assert.assertEquals(ClickHouseUtils.getLeadingComment(" "), "");
        Assert.assertEquals(ClickHouseUtils.getLeadingComment("a"), "");
        Assert.assertEquals(ClickHouseUtils.getLeadingComment("-- a \r\nselect 1"), "a");
        Assert.assertEquals(ClickHouseUtils.getLeadingComment("   -- a \r\n-- b\nselect 1"), "a");
        Assert.assertEquals(ClickHouseUtils.getLeadingComment("/* a */select 1"), "a");
        Assert.assertEquals(ClickHouseUtils.getLeadingComment("  /* a /* b */*/ /*-- b*/\nselect 1"),
                "a /* b */");
        Assert.assertEquals(ClickHouseUtils.getLeadingComment("select /* a */1"), "");
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
        Assert.assertEquals(ClickHouseUtils.skipBrackets(newArgs = ")]", 0, newArgs.length(), '['),
                newArgs.length());
        Assert.assertEquals(ClickHouseUtils.skipBrackets(newArgs = "{}", 0, newArgs.length(), '{'),
                newArgs.length());
        Assert.assertEquals(ClickHouseUtils.skipBrackets(newArgs = "{ '''\\'}'}", 0, newArgs.length(), '{'),
                newArgs.length());
        Assert.assertEquals(
                ClickHouseUtils.skipBrackets(newArgs = "{ -- }\n/*/*}*/*/}", 0, newArgs.length(), '{'),
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
        Assert.assertEquals(ClickHouseUtils.skipQuotedString(newArgs, 0, newArgs.length(), '\''),
                newArgs.length());
        Assert.assertEquals(ClickHouseUtils.skipQuotedString(newArgs = "''''''", 0, newArgs.length(), '\''),
                newArgs.length());
        Assert.assertEquals(ClickHouseUtils.skipQuotedString(newArgs = "'\\''''", 0, newArgs.length(), '\''),
                newArgs.length());
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ClickHouseUtils.skipQuotedString("", 0, 0, '\''));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ClickHouseUtils.skipQuotedString("'", 0, 1, '\''));
        Assert.assertEquals(ClickHouseUtils.skipQuotedString(newArgs = "''", 0, newArgs.length(), '\''),
                newArgs.length());
        Assert.assertEquals(ClickHouseUtils.skipQuotedString(newArgs = " '''\\'}'", 0, newArgs.length(), '\''),
                newArgs.indexOf('\\'));
        Assert.assertEquals(ClickHouseUtils.skipQuotedString(newArgs, 1, newArgs.length(), '\''),
                newArgs.length());
        Assert.assertEquals(ClickHouseUtils.skipQuotedString(newArgs, 1, newArgs.length(), '\''),
                newArgs.length());
        Assert.assertEquals(ClickHouseUtils.skipQuotedString(newArgs = "'\\\\'''", 0, newArgs.length(), '\''),
                newArgs.length());
    }

    @Test(groups = { "unit" })
    public void testSkipSingleLineComment() {
        String args = "select 1 -- select one\n union all select 2 -- select two--";
        Assert.assertEquals(ClickHouseUtils.skipSingleLineComment(args, 11, args.length()),
                args.indexOf('\n') + 1);
        Assert.assertEquals(ClickHouseUtils.skipSingleLineComment(args, args.indexOf("--", 11), args.length()),
                args.length());
    }

    @Test(groups = { "unit" })
    public void testSkipMultipleLineComment() {
        String args = "select 1 /* select 1/*one*/ -- a */, 2";
        Assert.assertEquals(ClickHouseUtils.skipMultiLineComment(args, 11, args.length()),
                args.lastIndexOf("*/") + 2);
        Assert.assertEquals(ClickHouseUtils.skipMultiLineComment(args, 21, args.length()),
                args.indexOf("*/", 21) + 2);
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ClickHouseUtils.skipMultiLineComment(args, args.lastIndexOf("*/") + 1,
                        args.length()));
    }

    @Test(groups = { "unit" })
    public void testSkipContentsUntilCharacters() {
        String args = "select 'a' as `--b`,1/*('1(/*'*/(\0*/ \0from number(10)";
        Assert.assertEquals(ClickHouseUtils.skipContentsUntil(args, 0, args.length()),
                args.lastIndexOf('\0') + 1);
        Assert.assertEquals(ClickHouseUtils.skipContentsUntil(args, 0, args.length(), '('),
                args.lastIndexOf('(') + 1);
        Assert.assertEquals(ClickHouseUtils.skipContentsUntil(args, 0, args.length(), '(', 'n'),
                args.lastIndexOf('n') + 1);

        args = "column1 AggregateFunction(quantiles(0.5, 0.9), UInt64),\ncolumn2 UInt8 not null";
        Assert.assertEquals(ClickHouseUtils.skipContentsUntil(args, 0, args.length()), args.length());
        Assert.assertEquals(ClickHouseUtils.skipContentsUntil(args, 0, args.length(), ','),
                args.lastIndexOf(',') + 1);
        Assert.assertEquals(ClickHouseUtils.skipContentsUntil(args, 0, args.length(), '@'), args.length());
    }

    @Test(groups = { "unit" })
    public void testSkipContentsUntilKeyword() {
        String args = "select 'a' as `--b`,1/*('1(/*'*/(\0*/ \0from number(10)";
        Assert.assertEquals(ClickHouseUtils.skipContentsUntil(args, 0, args.length(), (String) null, true), 1);
        Assert.assertEquals(ClickHouseUtils.skipContentsUntil(args, 0, args.length(), (String) null, false), 1);
        Assert.assertEquals(ClickHouseUtils.skipContentsUntil(args, 0, args.length(), "", true), 1);
        Assert.assertEquals(ClickHouseUtils.skipContentsUntil(args, 0, args.length(), "", false), 1);
        Assert.assertEquals(ClickHouseUtils.skipContentsUntil(args, 0, args.length(), "SELECT", true),
                args.length());
        Assert.assertEquals(ClickHouseUtils.skipContentsUntil(args, 0, args.length(), "SELECT", false),
                args.indexOf(' '));
        Assert.assertEquals(ClickHouseUtils.skipContentsUntil(args, 0, args.length(), "'", true),
                args.indexOf('a'));
        Assert.assertEquals(ClickHouseUtils.skipContentsUntil(args, 0, args.length(), "'", false),
                args.indexOf('a'));
        Assert.assertEquals(ClickHouseUtils.skipContentsUntil(args, 0, args.length(), "From", true),
                args.length());
        Assert.assertEquals(ClickHouseUtils.skipContentsUntil(args, 0, args.length(), "From", false),
                args.lastIndexOf("from") + 4);

        args = "with (SELECT 1 as a) abcb -- select\nselect abcd";
        Assert.assertEquals(ClickHouseUtils.skipContentsUntil(args, 0, args.length(), "SELECT", true),
                args.length());
        Assert.assertEquals(ClickHouseUtils.skipContentsUntil(args, 0, args.length(), "SELECT", false),
                args.lastIndexOf(' '));
        Assert.assertEquals(ClickHouseUtils.skipContentsUntil(args, 0, args.length(), "abcd", true),
                args.length());
        Assert.assertEquals(ClickHouseUtils.skipContentsUntil(args, 0, args.length(), "abcd", false),
                args.length());
        Assert.assertEquals(ClickHouseUtils.skipContentsUntil("abcd", 0, args.length(), "abcd", true), 4);
        Assert.assertEquals(ClickHouseUtils.skipContentsUntil("abcd", 0, args.length(), "abcd", false), 4);

        args = "column1 AggregateFunction(quantiles(0.5, 0.9), UInt64),\ncolumn2 UInt64 not null";
        Assert.assertEquals(ClickHouseUtils.skipContentsUntil(args, 0, args.length(), "uint128", true),
                args.length());
        Assert.assertEquals(ClickHouseUtils.skipContentsUntil(args, 0, args.length(), "uint128", false),
                args.length());
        Assert.assertEquals(ClickHouseUtils.skipContentsUntil(args, 0, args.length(), "uint64", true),
                args.length());
        Assert.assertEquals(ClickHouseUtils.skipContentsUntil(args, 0, args.length(), "uint64", false),
                args.lastIndexOf("UInt64") + 6);
    }

    @Test(groups = { "unit" })
    public void testSkipContentsUntilKeywords() {
        String args = "select 1 Insert, 2 as into";
        Assert.assertEquals(ClickHouseUtils.skipContentsUntil(args, 0, args.length(), (String[]) null, true),
                1);
        Assert.assertEquals(ClickHouseUtils.skipContentsUntil(args, 0, args.length(), (String[]) null, false),
                1);
        Assert.assertEquals(ClickHouseUtils.skipContentsUntil(args, 0, args.length(), new String[0], true), 1);
        Assert.assertEquals(ClickHouseUtils.skipContentsUntil(args, 0, args.length(), new String[0], false), 1);
        Assert.assertEquals(
                ClickHouseUtils.skipContentsUntil(args, 0, args.length(), new String[] { null }, true),
                1);
        Assert.assertEquals(
                ClickHouseUtils.skipContentsUntil(args, 0, args.length(), new String[] { null }, false),
                1);
        Assert.assertEquals(
                ClickHouseUtils.skipContentsUntil(args, 0, args.length(), new String[] { "" }, true),
                1);
        Assert.assertEquals(
                ClickHouseUtils.skipContentsUntil(args, 0, args.length(), new String[] { "" }, false),
                1);

        Assert.assertEquals(
                ClickHouseUtils.skipContentsUntil(args, 0, args.length(),
                        new String[] { "1", "insert" }, true),
                args.length());
        Assert.assertEquals(
                ClickHouseUtils.skipContentsUntil(args, 0, args.length(),
                        new String[] { "1", "insert" }, false),
                args.indexOf(','));
        Assert.assertEquals(
                ClickHouseUtils.skipContentsUntil(args, 0, args.length(),
                        new String[] { " ", "insert" }, true),
                args.length());
        Assert.assertEquals(
                ClickHouseUtils.skipContentsUntil(args, 0, args.length(),
                        new String[] { " ", "insert" }, false),
                args.length());
        Assert.assertEquals(
                ClickHouseUtils.skipContentsUntil(args, 0, args.length(), new String[] { "1", null },
                        true),
                args.indexOf('I'));
        Assert.assertEquals(
                ClickHouseUtils.skipContentsUntil(args, 0, args.length(), new String[] { "1", null },
                        false),
                args.indexOf('I'));

        args = "insert Into db.table(c1, c2) select d2, d3 From input('d1 String, d2 UInt8, d3 Array(UInt16)')";
        Assert.assertEquals(
                ClickHouseUtils.skipContentsUntil(args, 0, args.length(),
                        new String[] { "insert", "into" }, true),
                args.length());
        Assert.assertEquals(
                ClickHouseUtils.skipContentsUntil(args, 0, args.length(),
                        new String[] { "insert", "into" }, false),
                args.indexOf('d') - 1);
        Assert.assertEquals(
                ClickHouseUtils.skipContentsUntil(args, 0, args.length(),
                        new String[] { "from", "input" }, true),
                args.length());
        Assert.assertEquals(
                ClickHouseUtils.skipContentsUntil(args, 0, args.length(),
                        new String[] { "from", "input" }, false),
                args.indexOf('\'') - 1);
    }

    @Test(groups = { "unit" })
    public void testReadNameOrQuotedString() {
        String args = "123";
        StringBuilder builder = new StringBuilder();
        Assert.assertEquals(ClickHouseUtils.readNameOrQuotedString(args, 0, args.length(), builder),
                args.length());
        Assert.assertEquals(builder.toString(), args);

        builder.setLength(0);
        Assert.assertEquals(ClickHouseUtils.readNameOrQuotedString(args = " 123", 1, args.length(), builder),
                args.length());
        Assert.assertEquals(builder.toString(), "123");

        builder.setLength(0);
        Assert.assertEquals(
                ClickHouseUtils.readNameOrQuotedString(args = " `1\"'2``3` ", 1, args.length(),
                        builder),
                args.lastIndexOf('`') + 1);
        Assert.assertEquals(builder.toString(), "1\"'2`3");

        builder.setLength(0);
        Assert.assertEquals(ClickHouseUtils.readNameOrQuotedString(args = "input--", 0, args.length(), builder),
                args.indexOf('-'));
        Assert.assertEquals(builder.toString(), "input");
        builder.setLength(0);
        Assert.assertEquals(ClickHouseUtils.readNameOrQuotedString(args = "input/*", 0, args.length(), builder),
                args.indexOf('/'));
        Assert.assertEquals(builder.toString(), "input");
    }

    @Test(groups = { "unit" })
    public void testReadEnumValues() {
        String args = "Enum( ' `''1\" ' = 1, '\\''=2 )";
        Map<String, Integer> values = new HashMap<>();
        Assert.assertEquals(ClickHouseUtils.readEnumValues(args, 5, args.length(), values),
                args.lastIndexOf(')') + 1);
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
        Assert.assertEquals(ClickHouseUtils.readEnumValues(args, 7, args.length(), values),
                args.lastIndexOf(','));
        Assert.assertEquals(values.size(), 1);
        Assert.assertEquals(values.get("1)"), (Integer) 2);
    }

    @Test(groups = { "unit" })
    public void testReadValueArray() {
        String args = " [1, 2,   3 ] ";
        List<String> list = new LinkedList<>();
        Assert.assertEquals(ClickHouseUtils.readValueArray(args, args.indexOf('['), args.length(), list::add),
                args.lastIndexOf(']') + 1);
        Assert.assertEquals(list, Arrays.asList("1", "2", "3"));

        args = "['1\\'2', '2,3' , '3\n4\r5']";
        list.clear();
        Assert.assertEquals(ClickHouseUtils.readValueArray(args, 0, args.length(), list::add), args.length());
        Assert.assertEquals(list, Arrays.asList("1'2", "2,3", "3\n4\r5"));

        args = "[[1,2,3],[],(),(4,5,6)]";
        list.clear();
        Assert.assertEquals(ClickHouseUtils.readValueArray(args, 0, args.length(), list::add), args.length());
        Assert.assertEquals(list, Arrays.asList("[1,2,3]", "[]", "()", "(4,5,6)"));

        args = "[,null,nan,-inf,,123,]";
        list.clear();
        Assert.assertEquals(ClickHouseUtils.readValueArray(args, 0, args.length(), list::add), args.length());
        Assert.assertEquals(list, Arrays.asList(null, null, "nan", "-inf", null, "123", null));

        args = "1";
        list.clear();
        Assert.assertEquals(ClickHouseUtils.readValueArray(args, 0, args.length(), list::add), args.length());
        Assert.assertEquals(list, Arrays.asList("1"));

        args = "";
        list.clear();
        Assert.assertEquals(ClickHouseUtils.readValueArray(args, 0, args.length(), list::add), 0);
        Assert.assertEquals(list, Collections.emptyList());

        args = " [                              ]   ]";
        list.clear();
        Assert.assertEquals(ClickHouseUtils.readValueArray(args, 0, args.length(), list::add), args.indexOf(']') + 1);
        Assert.assertEquals(list, Collections.emptyList());
    }

    @Test(groups = { "unit" })
    public void testReadParameters() {
        String args = "column1 AggregateFunction( quantiles(0.5, 'c \\'''([1],2) d',0.9) , UInt64),\ncolumn2 UInt8 not null";
        List<String> params = new LinkedList<>();
        Assert.assertEquals(ClickHouseUtils.readParameters(args, args.indexOf('('), args.length(), params),
                args.lastIndexOf(')') + 1);
        Assert.assertEquals(params, Arrays.asList("quantiles(0.5,'c \\'''([1],2) d',0.9)", "UInt64"));

        params.clear();
        args = "   ('a'/* a*/, 1-- test\n, b)";
        Assert.assertEquals(ClickHouseUtils.readParameters(args, 0, args.length(), params), args.length());
        Assert.assertEquals(params, Arrays.asList("'a'", "1", "b"));

        params.clear();
        args = " a, b c";
        Assert.assertEquals(ClickHouseUtils.readParameters(args, 0, args.length(), params), args.length());
        Assert.assertEquals(params, Arrays.asList("a", "bc"));
    }

    @Test(groups = { "unit" })
    public void testFileInputStream() {
        Assert.assertThrows(FileNotFoundException.class,
                () -> ClickHouseUtils.getFileInputStream(UUID.randomUUID().toString()));
    }

    @Test(groups = { "unit" })
    public void testParseJson() {
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseUtils.parseJson(null));
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseUtils.parseJson(""));

        // constants
        Assert.assertEquals(ClickHouseUtils.parseJson(" true"), Boolean.TRUE);
        Assert.assertEquals(ClickHouseUtils.parseJson("false "), Boolean.FALSE);
        Assert.assertEquals(ClickHouseUtils.parseJson(" null "), null);

        // numbers
        Assert.assertEquals(ClickHouseUtils.parseJson("1"), 1);
        Assert.assertEquals(ClickHouseUtils.parseJson("-1"), -1);
        Assert.assertEquals(ClickHouseUtils.parseJson("1.1"), 1.1F);
        Assert.assertEquals(ClickHouseUtils.parseJson("-1.1"), -1.1F);
        Assert.assertEquals(ClickHouseUtils.parseJson("123456789.1"), 123456789.1D);
        Assert.assertEquals(ClickHouseUtils.parseJson("-123456789.1"), -123456789.1D);

        // string
        Assert.assertEquals(ClickHouseUtils.parseJson("\"\""), "");
        Assert.assertEquals(ClickHouseUtils.parseJson(" \" a \" "), " a ");
        Assert.assertEquals(ClickHouseUtils.parseJson(" \"\\a\\/\" "), "a/");

        // array
        Assert.assertEquals(ClickHouseUtils.parseJson("[]"), new Object[0]);
        Assert.assertEquals(ClickHouseUtils.parseJson(" [ ] "), new Object[0]);
        Assert.assertEquals(ClickHouseUtils.parseJson("[1,2]"), new Object[] { 1, 2 });
        Assert.assertEquals(ClickHouseUtils.parseJson("[1, -2, true, 1.1, -2.1, \"ccc\"]"),
                new Object[] { 1, -2, Boolean.TRUE, 1.1F, -2.1F, "ccc" });

        // object
        Assert.assertEquals(ClickHouseUtils.parseJson("{}"), Collections.emptyMap());
        Assert.assertEquals(ClickHouseUtils.parseJson(" { } "), Collections.emptyMap());

        Map<String, Object> map = new HashMap<>();
        map.put("a", 1);
        map.put("b", 2.2F);
        map.put("c", null);
        Assert.assertEquals(ClickHouseUtils.parseJson("{\"a\" : 1, \"c\": null, \"b\":2.2}"), map);

        map.clear();
        map.put("read_rows", "1");
        map.put("read_bytes", "12345678901");
        map.put("written_rows", "0.0");
        map.put("written_bytes", "0");
        map.put("total_rows_to_read", "233");
        Assert.assertEquals(ClickHouseUtils.parseJson(
                "{\"read_rows\":\"1\",\"read_bytes\":\"12345678901\",\"written_rows\":\"0.0\",\"written_bytes\":\"0\",\"total_rows_to_read\":\"233\"}"),
                map);

        // mixed
        map.clear();
        map.put("a", 1);
        map.put("b", 2.2F);
        map.put("c", null);
        Assert.assertEquals(ClickHouseUtils.parseJson("[null, 1, [2,3], {\"a\" : 1, \"c\": null, \"b\":2.2}]"),
                new Object[] { null, 1, new Object[] { 2, 3 }, map });
    }
}

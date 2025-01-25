package com.clickhouse.data;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHouseUtilsTest {
    @Test(groups = { "unit" })
    public void testCreateTempFile() throws IOException {
        File f = ClickHouseUtils.createTempFile(null, null);
        Assert.assertNotNull(f);
        Assert.assertTrue(f.exists(), f.getAbsolutePath() + " should exist");
        Assert.assertTrue(f.getName().endsWith(".data"),
                "By default temporary file should end with .data, but it's " + f.getName());

        f = ClickHouseUtils.createTempFile("prefix__", "__suffix", true);
        Assert.assertNotNull(f);
        Assert.assertTrue(f.exists(), f.getAbsolutePath() + " should exist");
        Assert.assertTrue(f.getName().startsWith("prefix__"),
                "The temporary file should start with prefix__, but it's " + f.getName());
        Assert.assertTrue(f.getName().endsWith("__suffix"),
                "The temporary file should end with __suffix, but it's " + f.getName());
    }

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
    public void testExtractParameters() {
        Map<String, String> expected = new HashMap<>();
        expected.put("*", "true");
        expected.put("a", "1");
        expected.put("b", "false");
        Assert.assertEquals(ClickHouseUtils.extractParameters(null, null), new HashMap<>());
        Assert.assertEquals(ClickHouseUtils.extractParameters("", null), new HashMap<>());
        Assert.assertEquals(ClickHouseUtils.extractParameters("*&a=1&!b", null), expected);
        Assert.assertEquals(ClickHouseUtils.extractParameters("*&a=1&!b", new HashMap<>()), expected);
        Assert.assertEquals(ClickHouseUtils.extractParameters("*&a=1&!b", expected), expected);
    }

    @Test(groups = { "unit" })
    public void testNewInstance() {
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseUtils.newInstance(null, null, null));
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseUtils.newInstance("", Object.class, null));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ClickHouseUtils.newInstance("java.util.List", Object.class, null));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ClickHouseUtils.newInstance("java.util.NoSuchListClass", Object.class, null));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ClickHouseUtils.newInstance("java.lang.Object", ArrayList.class, null));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ClickHouseUtils.newInstance("java.util.ArrayList", Collections.class, null));

        Assert.assertEquals(ClickHouseUtils.newInstance("java.util.ArrayList", List.class, null).getClass(),
                ArrayList.class);
    }

    @Test(groups = { "unit" })
    public void testGetFile() throws IOException {
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseUtils.getFile(null));
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseUtils.getFile(""));

        Assert.assertTrue(ClickHouseUtils.getFile("README.md").toFile().exists());

        Assert.assertTrue(ClickHouseUtils.getFile("README.md").toFile().exists());
        Assert.assertTrue(ClickHouseUtils.getFile("../README.md").toFile().exists());

        Assert.assertEquals(ClickHouseUtils.getFile("~/a.csv"), Paths.get(System.getProperty("user.home"), "a.csv"));
        Assert.assertEquals(ClickHouseUtils.getFile("~/../a.csv"),
                Paths.get(System.getProperty("user.home"), "../a.csv").normalize());
    }

    @Test(groups = { "unit" })
    public void testFindFiles() throws IOException {
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseUtils.findFiles(null));
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseUtils.findFiles(""));

        if (System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("windows")) {
            Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseUtils.findFiles("READM?.md"));
            Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseUtils.findFiles("READM<?.md"));
            Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseUtils.findFiles("READM>.md"));
            Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseUtils.findFiles("READM|.md"));
            Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseUtils.findFiles("READM*.md"));
            Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseUtils.findFiles("READM<>:\\\"|?*.md"));
            Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseUtils.findFiles(" "));
        }
        else {
            Assert.assertEquals(ClickHouseUtils.findFiles("READM?.md").size(), 1);
        }

        Assert.assertEquals(ClickHouseUtils.findFiles("README.md").size(), 1);
        Assert.assertEquals(ClickHouseUtils.findFiles("glob:*.md").size(), 1);
        Assert.assertTrue(ClickHouseUtils.findFiles("glob:**.java", "src", "..").size() >= 1);
        Assert.assertTrue(ClickHouseUtils.findFiles("glob:**.java", "src/test").size() >= 1);
        Assert.assertTrue(ClickHouseUtils.findFiles("glob:../*.md", "../").size() >= 1);
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
        TestServiceInterface service = null;
        try {
            service = ClickHouseUtils.getService(TestServiceInterface.class, null);
        } catch (Exception e) {
            Assert.fail("Should not fail");
        }

        Assert.assertNotNull(service);

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
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseUtils.skipMultiLineComment("", 0, 0));
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseUtils.skipMultiLineComment("/", 0, 1));
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseUtils.skipMultiLineComment("/*", 0, 2));
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseUtils.skipMultiLineComment("/**", 0, 3));
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseUtils.skipMultiLineComment("/*/*/", 0, 5));
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseUtils.skipMultiLineComment("/*/**/", 0, 6));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ClickHouseUtils.skipMultiLineComment("/*/***/", 0, 7));

        Assert.assertEquals(ClickHouseUtils.skipMultiLineComment("/**/", 1, 4), 4);
        Assert.assertEquals(ClickHouseUtils.skipMultiLineComment("/*/**/*/", 2, 8), 8);
        Assert.assertEquals(ClickHouseUtils.skipMultiLineComment("/*/*/**/*/*/", 2, 12), 12);

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

        args = "[1, null , 3,, Null]";
        list.clear();
        Assert.assertEquals(ClickHouseUtils.readValueArray(args, 0, args.length(), list::add), args.length());
        Assert.assertEquals(list, Arrays.asList("1", null, "3", null, null));

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
        Assert.assertEquals(ClickHouseUtils.readValueArray(args, 0, args.length(), list::add),
                args.indexOf(']') + 1);
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
        args = " a = 2 -- enum value\n, /** type declaration **/ b  c  , `d` /*e*/ --f";
        Assert.assertEquals(ClickHouseUtils.readParameters(args, 0, args.length(), params), args.length());
        Assert.assertEquals(params, Arrays.asList("a=2", "b c", "`d`"));

        params.clear();
        args = "column1 SimpleAggregateFunction(anyLast, Nested(a string, b string))";
        Assert.assertEquals(ClickHouseUtils.readParameters(args, args.indexOf('('), args.length(), params),
                args.lastIndexOf(')') + 1);
        Assert.assertEquals(params, Arrays.asList("anyLast", "Nested(a string,b string)"));
    }

    @Test(groups = { "unit" })
    public void testRemove() {
        Assert.assertEquals(ClickHouseUtils.remove(null, '\0'), "");
        Assert.assertEquals(ClickHouseUtils.remove("", '\0'), "");
        Assert.assertEquals(ClickHouseUtils.remove("test", '_'), "test");
        Assert.assertEquals(ClickHouseUtils.remove("test", '_', 'x'), "test");
        Assert.assertEquals(ClickHouseUtils.remove("test", '_', 't', 'e', 's', 'x'), "");
        Assert.assertEquals(ClickHouseUtils.remove("test_1", '_'), "test1");
        Assert.assertEquals(ClickHouseUtils.remove("\t te s t_1 \t", '_', '\t', ' '), "test1");
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

    @Test(groups = { "unit" })
    public void testToJavaByteArrayExpression() {
        Assert.assertEquals(ClickHouseUtils.toJavaByteArrayExpression(null), "null");
        Assert.assertEquals(ClickHouseUtils.toJavaByteArrayExpression(new byte[0]), "{}");
        Assert.assertEquals(ClickHouseUtils.toJavaByteArrayExpression(new byte[3]),
                "{(byte)0x00,(byte)0x00,(byte)0x00}");
        byte[] bytes = new byte[256];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) i;
        }
        Assert.assertEquals(ClickHouseUtils.toJavaByteArrayExpression(bytes),
                "{(byte)0x00,(byte)0x01,(byte)0x02,(byte)0x03,(byte)0x04,(byte)0x05,(byte)0x06,(byte)0x07,(byte)0x08,(byte)0x09,(byte)0x0A,(byte)0x0B,(byte)0x0C,(byte)0x0D,(byte)0x0E,(byte)0x0F,(byte)0x10,(byte)0x11,(byte)0x12,(byte)0x13,(byte)0x14,(byte)0x15,(byte)0x16,(byte)0x17,(byte)0x18,(byte)0x19,(byte)0x1A,(byte)0x1B,(byte)0x1C,(byte)0x1D,(byte)0x1E,(byte)0x1F,(byte)0x20,(byte)0x21,(byte)0x22,(byte)0x23,(byte)0x24,(byte)0x25,(byte)0x26,(byte)0x27,(byte)0x28,(byte)0x29,(byte)0x2A,(byte)0x2B,(byte)0x2C,(byte)0x2D,(byte)0x2E,(byte)0x2F,(byte)0x30,(byte)0x31,(byte)0x32,(byte)0x33,(byte)0x34,(byte)0x35,(byte)0x36,(byte)0x37,(byte)0x38,(byte)0x39,(byte)0x3A,(byte)0x3B,(byte)0x3C,(byte)0x3D,(byte)0x3E,(byte)0x3F,(byte)0x40,(byte)0x41,(byte)0x42,(byte)0x43,(byte)0x44,(byte)0x45,(byte)0x46,(byte)0x47,(byte)0x48,(byte)0x49,(byte)0x4A,(byte)0x4B,(byte)0x4C,(byte)0x4D,(byte)0x4E,(byte)0x4F,(byte)0x50,(byte)0x51,(byte)0x52,(byte)0x53,(byte)0x54,(byte)0x55,(byte)0x56,(byte)0x57,(byte)0x58,(byte)0x59,(byte)0x5A,(byte)0x5B,(byte)0x5C,(byte)0x5D,(byte)0x5E,(byte)0x5F,(byte)0x60,(byte)0x61,(byte)0x62,(byte)0x63,(byte)0x64,(byte)0x65,(byte)0x66,(byte)0x67,(byte)0x68,(byte)0x69,(byte)0x6A,(byte)0x6B,(byte)0x6C,(byte)0x6D,(byte)0x6E,(byte)0x6F,(byte)0x70,(byte)0x71,(byte)0x72,(byte)0x73,(byte)0x74,(byte)0x75,(byte)0x76,(byte)0x77,(byte)0x78,(byte)0x79,(byte)0x7A,(byte)0x7B,(byte)0x7C,(byte)0x7D,(byte)0x7E,(byte)0x7F,(byte)0x80,(byte)0x81,(byte)0x82,(byte)0x83,(byte)0x84,(byte)0x85,(byte)0x86,(byte)0x87,(byte)0x88,(byte)0x89,(byte)0x8A,(byte)0x8B,(byte)0x8C,(byte)0x8D,(byte)0x8E,(byte)0x8F,(byte)0x90,(byte)0x91,(byte)0x92,(byte)0x93,(byte)0x94,(byte)0x95,(byte)0x96,(byte)0x97,(byte)0x98,(byte)0x99,(byte)0x9A,(byte)0x9B,(byte)0x9C,(byte)0x9D,(byte)0x9E,(byte)0x9F,(byte)0xA0,(byte)0xA1,(byte)0xA2,(byte)0xA3,(byte)0xA4,(byte)0xA5,(byte)0xA6,(byte)0xA7,(byte)0xA8,(byte)0xA9,(byte)0xAA,(byte)0xAB,(byte)0xAC,(byte)0xAD,(byte)0xAE,(byte)0xAF,(byte)0xB0,(byte)0xB1,(byte)0xB2,(byte)0xB3,(byte)0xB4,(byte)0xB5,(byte)0xB6,(byte)0xB7,(byte)0xB8,(byte)0xB9,(byte)0xBA,(byte)0xBB,(byte)0xBC,(byte)0xBD,(byte)0xBE,(byte)0xBF,(byte)0xC0,(byte)0xC1,(byte)0xC2,(byte)0xC3,(byte)0xC4,(byte)0xC5,(byte)0xC6,(byte)0xC7,(byte)0xC8,(byte)0xC9,(byte)0xCA,(byte)0xCB,(byte)0xCC,(byte)0xCD,(byte)0xCE,(byte)0xCF,(byte)0xD0,(byte)0xD1,(byte)0xD2,(byte)0xD3,(byte)0xD4,(byte)0xD5,(byte)0xD6,(byte)0xD7,(byte)0xD8,(byte)0xD9,(byte)0xDA,(byte)0xDB,(byte)0xDC,(byte)0xDD,(byte)0xDE,(byte)0xDF,(byte)0xE0,(byte)0xE1,(byte)0xE2,(byte)0xE3,(byte)0xE4,(byte)0xE5,(byte)0xE6,(byte)0xE7,(byte)0xE8,(byte)0xE9,(byte)0xEA,(byte)0xEB,(byte)0xEC,(byte)0xED,(byte)0xEE,(byte)0xEF,(byte)0xF0,(byte)0xF1,(byte)0xF2,(byte)0xF3,(byte)0xF4,(byte)0xF5,(byte)0xF6,(byte)0xF7,(byte)0xF8,(byte)0xF9,(byte)0xFA,(byte)0xFB,(byte)0xFC,(byte)0xFD,(byte)0xFE,(byte)0xFF}");
    }

    @Test(groups = { "unit" })
    public void testWaitFor() throws Exception {
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseUtils.waitFor(null, 1L, TimeUnit.SECONDS));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ClickHouseUtils.waitFor(new AtomicBoolean(true), 1L, null));

        final AtomicBoolean flag = new AtomicBoolean(false);
        Assert.assertFalse(ClickHouseUtils.waitFor(flag, 1L, TimeUnit.MILLISECONDS));
        new Thread(() -> {
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            flag.set(true);
        }).start();
        Assert.assertFalse(ClickHouseUtils.waitFor(flag, 100L, TimeUnit.MILLISECONDS));
        Assert.assertTrue(ClickHouseUtils.waitFor(flag, 2L, TimeUnit.SECONDS));
    }
}

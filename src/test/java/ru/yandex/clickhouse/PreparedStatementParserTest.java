package ru.yandex.clickhouse;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link PreparedStatementParser}
 */
public class PreparedStatementParserTest {

    @Test
    public void testNullSafety() {
        try {
            PreparedStatementParser.parse(null);
            Assert.fail();
        } catch (IllegalArgumentException iae) { /* expected */ }
    }

    @Test
    public void testParseSimple() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t (a, b) VALUES (?, ?)");
        assertMatchParams(new String[][] {{"?", "?"}}, s);
    }

    @Test
    public void testParseConstantSimple() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t (a, b) VALUES (?, 'foo')");
        assertMatchParams(new String[][] {{"?", "'foo'"}}, s);
    }

    @Test
    public void testParseSimpleWhitespaceValueMode() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "    INSERT\t INTO t(a, b)    VALUES(?, ?)");
        assertMatchParams(new String[][] {{"?", "?"}}, s);
    }

    @Test
    public void testParseConstantSimpleInt() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t (a, b) VALUES (?, 42)");
        assertMatchParams(new String[][] {{"?", "42"}}, s);
    }

    @Test
    public void testParseConstantSimpleIntTrailingWhitespace() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t (a, b) VALUES (?,42 )");
        assertMatchParams(new String[][] {{"?", "42"}}, s);
    }

    @Test
    public void testParseConstantSimpleIntTrailingLeadingWhitespace() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t (a, b) VALUES (?, 42 )");
        assertMatchParams(new String[][] {{"?", "42"}}, s);
    }

    @Test
    public void testParseParentheses() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t (a, b) VALUES ((?), ('foo'))");
        assertMatchParams(new String[][] {{"?", "'foo'"}}, s);
    }

    @Test
    public void testParseParenthesesInQuotes() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t (a, b) VALUES ((?), ('foo)))'))");
        assertMatchParams(new String[][] {{"?", "'foo)))'"}}, s);
    }

    @Test
    public void testParseEscapedQuote() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t (a, b) VALUES (?, 'foo\\'bar')");
        assertMatchParams(new String[][] {{"?", "'foo\\'bar'"}}, s);
    }

    @Test
    public void testParseEscapedQuoteBroken() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t (a, b) VALUES (?, 'foo\'bar')");
        Assert.assertTrue(s.getParameters().isEmpty()); // Is this expected?
    }

    @Test
    public void testParseQuestionMarkInQuotes() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t (a, b) VALUES ('?', 'foo')");
        assertMatchParams(new String[][] {{"'?'", "'foo'"}}, s);
    }

    @Test
    public void testParseQuestionMarAndMoreInQuotes() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t (a, b) VALUES ('? foo ?', 'bar')");
        assertMatchParams(new String[][] {{"'? foo ?'", "'bar'"}}, s);
    }

    @Test
    public void testParseEscapedQuestionMark() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t (a, b) VALUES (\\?, 'foo')");
        assertMatchParams(new String[][] {{"'foo'"}}, s);
    }

    @Test
    public void testNoCommasQuestionMarks() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t (a, b) VALUES (foo ? bar ?)");
        List<List<String>> matrix = s.getParameters();
        Assert.assertEquals(1, matrix.size());
        Assert.assertEquals(1, matrix.get(0).size());
    }

    @Test
    public void testParseIgnoreInsert() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t (foo, ?) VALUES (?, 'bar')");
        assertMatchParams(new String[][] {{"?", "'bar'"}}, s);
    }

    @Test
    public void testDoubleComma() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t (a, b) VALUES (?, 'bar',, ?, , ?)");
        assertMatchParams(new String[][] {{"?", "'bar'", "?", "?"}}, s);
    }

    @Test
    public void testDoubleSingleQuote() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t (a) VALUES ('')");
        assertMatchParams(new String[][] {{"''"}}, s);
    }

    @Test
    public void testInsertNumbers() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t (foo, bar, baz) VALUES (42, 23, '42')");
        assertMatchParams(new String[][] {{"42", "23", "'42'"}}, s);
    }

    @Test
    public void testInsertBoolean() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t (foo, bar) VALUES (TRUE, false)");
        assertMatchParams(new String[][] {{"1", "0"}}, s);
    }

    @Test
    public void testMultiParams() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t (a, b) VALUES (?, ?), (?, ?)");
        assertMatchParams(
            new String[][] {
                {"?", "?" },
                {"?", "?" }
            },
            s);
    }

    @Test
    public void testMultiParamsWithConstants() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t (a, b) VALUES (?, 'foo'), ('bar', ?)");
        assertMatchParams(
            new String[][] {
                {"?", "'foo'" },
                {"'bar'", "?" }
            },
            s);
    }

    @Test
    public void testParseInsertSelect() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t (a, b) SELECT x, y");
        Assert.assertEquals("INSERT INTO t (a, b) SELECT x, y", s.getParts().get(0));
        Assert.assertTrue(s.getParameters().isEmpty());
    }

    @Test
    public void testParseInsertSelectParams() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t (a, b) SELECT x FROM u WHERE y = ? AND z = ?");
        Assert.assertEquals("INSERT INTO t (a, b) SELECT x FROM u WHERE y = ",
            s.getParts().get(0));
        Assert.assertEquals(" AND z = ", s.getParts().get(1));
        assertMatchParams(new String[][] {{"?",  "?"}}, s);
    }

    @Test
    public void testParseSelectGroupBy() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "SELECT SUM(x) FROM t WHERE y = ? GROUP BY ? HAVING COUNT(z) > ? ORDER BY z DESC");
        Assert.assertEquals("SELECT SUM(x) FROM t WHERE y = ",
            s.getParts().get(0));
        Assert.assertEquals(" GROUP BY ", s.getParts().get(1));
        Assert.assertEquals(" HAVING COUNT(z) > ",  s.getParts().get(2));
        Assert.assertEquals(" ORDER BY z DESC", s.getParts().get(3));
        assertMatchParams(new String[][] {{"?", "?", "?"}}, s);
    }

    @Test
    public void testParseWithComment1() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "select a --what is it?\nfrom t where a = ? and b = 1");
        Assert.assertEquals("select a --what is it?\nfrom t where a = ", s.getParts().get(0));
        Assert.assertEquals(" and b = 1", s.getParts().get(1));
        assertMatchParams(new String[][] {{"?"}}, s);
    }

    @Test
    public void testParseWithComment2() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "select a /*what is it?*/ from t where a = ? and b = 1");
        assertMatchParts(new String[] {
            "select a /*what is it?*/ from t where a = ",
            " and b = 1"},
            s);
        assertMatchParams(new String[][] {{"?"}}, s);
    }

    @Test
    public void testParseSelectStar() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "SELECT * FROM tbl");
        assertMatchParts(new String[] {"SELECT * FROM tbl"}, s);
        Assert.assertTrue(s.getParameters().isEmpty());
    }

    @Test
    public void testParseSelectStarParam() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "SELECT * FROM tbl WHERE t = ?");
        assertMatchParts(new String[] {"SELECT * FROM tbl WHERE t = "}, s);
        assertMatchParams(new String[][] {{"?"}}, s);
    }

    @Test
    public void testParseSelectEscapedGarbage() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "SELECT 'a\\'\\\\sdfasdf?adsf\\\\' as `sadf\\`?` FROM tbl WHERE t = ? AND r = ? ORDER BY 1");
        assertMatchParts(new String[] {
            "SELECT 'a\\'\\\\sdfasdf?adsf\\\\' as `sadf\\`?` FROM tbl WHERE t = ",
            " AND r = ",
            " ORDER BY 1"},
            s);
        assertMatchParams(new String[][] {{"?", "?"}}, s);
    }

    private static void assertMatchParts(String[] expected, PreparedStatementParser stmt) {
        List<String> parts = stmt.getParts();
        Assert.assertEquals(expected.length, parts.size());
        for (int i = 0; i < expected.length; i++) {
            Assert.assertEquals(expected[i], parts.get(i));
        }
    }

    private static void assertMatchParams(String[][] expected, PreparedStatementParser stmt) {
        List<List<String>> actual = stmt.getParameters();
        if (expected.length != actual.size()) {
            Assert.assertEquals(formatParams(expected), formatParamsList(actual));
        }
        if (expected.length == 0 && actual.isEmpty()) {
            return;
        }
        for (int i = 0; i < expected.length; i++) {
            String[] expRow = expected[i];
            String[] actRow = actual.get(i).toArray(new String[actual.get(i).size()]);
            Assert.assertEquals(expRow.length, actRow.length);
            for (int j = 0; j < expRow.length; j++) {
                Assert.assertEquals(expRow[j], actRow[j]);
            }
        }
    }

    private static String formatParamsList(List<List<String>> params) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < params.size(); i++) {
            sb.append("row ")
              .append(i)
              .append(": ")
              .append(formatRow(params.get(i).toArray(new String[params.get(i).size()])))
              .append("\n");
        }
        return sb.length() > 1 ?
            sb.substring(0, sb.length() - 1)
            : null;
    }

    private static String formatParams(String[][] params) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < params.length; i++) {
            sb.append("row ")
              .append(i)
              .append(": ")
              .append(formatRow(params[i]))
              .append("\n");
        }
        return sb.substring(0, sb.length() - 1);
    }

    private static String formatRow(String[] paramGroup) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < paramGroup.length; i++) {
            sb.append(i)
              .append(": ")
              .append(paramGroup[i])
              .append(", ");
        }
        return sb.substring(0,  sb.length() - 2);
    }

}

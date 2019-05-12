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
    public void testParseQuestionMarkAndMoreInQuotes() {
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
        Assert.assertEquals(matrix.size(), 1);
        Assert.assertEquals(matrix.get(0).size(), 1);
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
    public void testValuesModeDoubleQuotes() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t (\"foo.bar\") VALUES (?)");
        assertMatchParams(new String[][] {{"?"}}, s);
        Assert.assertEquals(s.getParts().get(0), "INSERT INTO t (\"foo.bar\") VALUES (");
    }

    @Test
    public void testValuesModeDoubleQuotesValues() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t (\"foo.bar\") VALUES (\"baz\")");
        assertMatchParams(new String[][] {{"\"baz\""}}, s);
        Assert.assertEquals(s.getParts().get(0), "INSERT INTO t (\"foo.bar\") VALUES (");
    }

    @Test
    public void testValuesModeSingleQuotes() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t ('foo.bar') VALUES (?)");
        assertMatchParams(new String[][] {{"?"}}, s);
        Assert.assertEquals(s.getParts().get(0), "INSERT INTO t ('foo.bar') VALUES (");
    }

    @Test
    public void testValuesModeSingleQuotesValues() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t ('foo.bar') VALUES ('baz')");
        assertMatchParams(new String[][] {{"'baz'"}}, s);
        Assert.assertEquals(s.getParts().get(0), "INSERT INTO t ('foo.bar') VALUES (");
    }

    @Test
    public void testParseInsertSelect() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t (a, b) SELECT x, y");
        Assert.assertEquals(s.getParts().get(0), "INSERT INTO t (a, b) SELECT x, y");
        Assert.assertTrue(s.getParameters().isEmpty());
    }

    @Test
    public void testParseInsertSelectParams() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO t (a, b) SELECT x FROM u WHERE y = ? AND z = ?");
        Assert.assertEquals(s.getParts().get(0),
            "INSERT INTO t (a, b) SELECT x FROM u WHERE y = ");
        Assert.assertEquals(" AND z = ", s.getParts().get(1));
        assertMatchParams(new String[][] {{"?",  "?"}}, s);
    }

    @Test
    public void testParseSelectGroupBy() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "SELECT SUM(x) FROM t WHERE y = ? GROUP BY ? HAVING COUNT(z) > ? ORDER BY z DESC");
        Assert.assertEquals("SELECT SUM(x) FROM t WHERE y = ",
            s.getParts().get(0));
        Assert.assertEquals(s.getParts().get(1), " GROUP BY ");
        Assert.assertEquals(s.getParts().get(2), " HAVING COUNT(z) > ");
        Assert.assertEquals(s.getParts().get(3), " ORDER BY z DESC");
        assertMatchParams(new String[][] {{"?", "?", "?"}}, s);
    }

    @Test
    public void testParseWithComment1() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "select a --what is it?\nfrom t where a = ? and b = 1");
        Assert.assertEquals( s.getParts().get(0), "select a --what is it?\nfrom t where a = ");
        Assert.assertEquals(s.getParts().get(1), " and b = 1");
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
        assertMatchParts(new String[] {"SELECT * FROM tbl WHERE t = ", ""}, s);
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

    @Test
    public void testRegularParam() throws Exception {
        // Test inspired by MetaBase test cases
        PreparedStatementParser s = PreparedStatementParser.parse(
            "SELECT count(*) AS `count` FROM `foo`.`bar` "
          + "WHERE (`foo`.`bar`.`id` <= 32 "
          + "AND (`foo`.`bar`.`name` like ?))");
        assertMatchParams(new String[][] {{"?"}}, s);
        Assert.assertEquals(
            s.getParts().get(0),
            "SELECT count(*) AS `count` FROM `foo`.`bar` "
          + "WHERE (`foo`.`bar`.`id` <= 32 "
          + "AND (`foo`.`bar`.`name` like ");
        Assert.assertEquals(
            s.getParts().get(1),
            "))");
    }

    @Test
    public void testRegularParamWhitespace() throws Exception {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "SELECT count(*) AS `count` FROM `foo`.`bar` "
          + "WHERE (`foo`.`bar`.`id` <= 32 "
          + "AND (`foo`.`bar`.`name` like ?   ))");
        assertMatchParams(new String[][] {{"?"}}, s);
        Assert.assertEquals(
            s.getParts().get(0),
            "SELECT count(*) AS `count` FROM `foo`.`bar` "
          + "WHERE (`foo`.`bar`.`id` <= 32 "
          + "AND (`foo`.`bar`.`name` like ");
        Assert.assertEquals(
            s.getParts().get(1),
            "   ))");
    }

    @Test
    public void testRegularParamInFunction() throws Exception {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "SELECT count(*) AS `count` FROM `foo`.`bar` "
          + "WHERE toMonday(`foo`.`bar`.`date`) = toMonday(?)");
        assertMatchParams(new String[][] {{"?"}}, s);
        Assert.assertEquals(
            s.getParts().get(0),
            "SELECT count(*) AS `count` FROM `foo`.`bar` "
          + "WHERE toMonday(`foo`.`bar`.`date`) = toMonday(");
        Assert.assertEquals(
            s.getParts().get(1),
            ")");
    }

    @Test
    public void testNullValuesSelect() throws Exception {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "SELECT 1 FROM foo WHERE bar IN (?, NULL)");
        List<List<String>> params = s.getParameters();
        Assert.assertEquals(params.size(), 1);
        Assert.assertEquals(params.get(0).size(), 1);
        Assert.assertEquals(params.get(0).get(0), "?");
    }

    @Test
    public void testNullValuesInsert() throws Exception {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO test.prep_nullable_value (s, i, f) VALUES "
          + "(?, NULL, ?), (NULL, null , ?)");
        assertMatchParams(new String[][] {
            {"?", "\\N", "?"},
            {"\\N", "\\N", "?"}},
            s);
    }

    @Test
    public void testParamLastCharacter() throws Exception {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "SELECT * FROM decisions "
          + "PREWHERE userID = ? "
          + "AND eventDate >= toDate(?) "
          + "AND eventDate <= toDate(?) "
          + "ORDER BY time DESC LIMIT ?, ?");
        assertMatchParams(new String[][] {{"?", "?", "?", "?", "?"}}, s);
        Assert.assertEquals(s.getParts().size(), 6);
        Assert.assertEquals(s.getParts().get(0), "SELECT * FROM decisions PREWHERE userID = ");
        Assert.assertEquals(s.getParts().get(1), " AND eventDate >= toDate(");
        Assert.assertEquals(s.getParts().get(2), ") AND eventDate <= toDate(");
        Assert.assertEquals(s.getParts().get(3), ") ORDER BY time DESC LIMIT ");
        Assert.assertEquals(s.getParts().get(4), ", ");
        Assert.assertEquals(s.getParts().get(5), "");
    }

    @Test
    public void testInsertValuesFunctions() throws Exception {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO foo(id, src, dst) "
          + "VALUES (?, IPv4ToIPv6(toIPv4(?)), IPv4ToIPv6(toIPv4(?)))");
        assertMatchParams(new String[][] {{ "?", "?", "?" }}, s);
        assertMatchParts(new String[] {
            "INSERT INTO foo(id, src, dst) VALUES (",
            ", IPv4ToIPv6(toIPv4(",
            ")), IPv4ToIPv6(toIPv4(",
            ")))"}, s);
    }

    @Test
    public void testMultiLineValues() {
        PreparedStatementParser s = PreparedStatementParser.parse(
            "INSERT INTO table1\n"
          + "\t(foo, bar)\r\n"
          + "\t\tVALUES\n"
          + "(?, ?) , \r\n"
          + "\t(?,?),(?,?)\n");
        Assert.assertTrue(s.isValuesMode());
        assertMatchParams(new String[][] {{"?", "?"}, {"?", "?"}, {"?", "?"}}, s);
        Assert.assertEquals(s.getParts().get(0),
            "INSERT INTO table1\n"
          + "\t(foo, bar)\r\n"
          + "\t\tVALUES\n"
          + "(");
        Assert.assertEquals(7, s.getParts().size());
        Assert.assertEquals(s.getParts().get(0),
            "INSERT INTO table1\n"
          + "\t(foo, bar)\r\n"
          + "\t\tVALUES\n"
          + "(");
        Assert.assertEquals(s.getParts().get(1), ", ");
        Assert.assertEquals(s.getParts().get(2),
            ") , \r\n"
          + "\t(");
        Assert.assertEquals(s.getParts().get(3), ",");
        Assert.assertEquals(s.getParts().get(4), "),(");
        Assert.assertEquals(s.getParts().get(5), ",");
        Assert.assertEquals(s.getParts().get(6), ")\n");
    }

    private static void assertMatchParts(String[] expected, PreparedStatementParser stmt) {
        List<String> parts = stmt.getParts();
        Assert.assertEquals( parts.size(), expected.length);
        for (int i = 0; i < expected.length; i++) {
            Assert.assertEquals(parts.get(i), expected[i]);
        }
    }

    private static void assertMatchParams(String[][] expected, PreparedStatementParser stmt) {
        List<List<String>> actual = stmt.getParameters();
        if (expected.length != actual.size()) {
            Assert.assertEquals(formatParamsList(actual), formatParams(expected));
        }
        if (expected.length == 0 && actual.isEmpty()) {
            return;
        }
        for (int i = 0; i < expected.length; i++) {
            String[] expRow = expected[i];
            String[] actRow = actual.get(i).toArray(new String[actual.get(i).size()]);
            Assert.assertEquals(actRow.length, expRow.length);
            for (int j = 0; j < expRow.length; j++) {
                Assert.assertEquals(actRow[j], expRow[j]);
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

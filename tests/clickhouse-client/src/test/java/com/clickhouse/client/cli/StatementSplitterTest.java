package com.clickhouse.client.cli;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class StatementSplitterTest {

    private static List<String> sqls(String text) {
        return StatementSplitter.split(text).stream().map(s -> s.sql).collect(Collectors.toList());
    }

    @DataProvider
    public Object[][] scripts() {
        return new Object[][] {
                {"SELECT 1", new String[] {"SELECT 1"}},
                {"SELECT 1;\nSELECT 2;\n", new String[] {"SELECT 1;", "SELECT 2;"}},
                {"SELECT 'a;b'; SELECT 2", new String[] {"SELECT 'a;b';", "SELECT 2"}},
                {"SELECT 'a\\';b'; SELECT 2", new String[] {"SELECT 'a\\';b';", "SELECT 2"}},
                {"SELECT 'a'';b'; SELECT 2", new String[] {"SELECT 'a'';b';", "SELECT 2"}},
                {"SELECT 1 AS `a;b`; SELECT 2", new String[] {"SELECT 1 AS `a;b`;", "SELECT 2"}},
                {"SELECT 1 -- a;b\n; SELECT 2", new String[] {"SELECT 1 -- a;b\n;", "SELECT 2"}},
                {"SELECT /* a;b */ 1; SELECT 2", new String[] {"SELECT /* a;b */ 1;", "SELECT 2"}},
                {"SELECT /* a /* b; */ c; */ 1; SELECT 2", new String[] {"SELECT /* a /* b; */ c; */ 1;", "SELECT 2"}},
                {"SELECT $$a;'b$$; SELECT $x$;$x$", new String[] {"SELECT $$a;'b$$;", "SELECT $x$;$x$"}},
                {"SELECT 1;\n-- the end\n", new String[] {"SELECT 1;"}},
                {"  \n-- only a comment\n", new String[] {}},
                // The tags header is skipped.
                {"-- Tags: no-fasttest\n-- Tag no-fasttest: reason\n\nSELECT 1;", new String[] {"SELECT 1;"}},
                // A leading comment stays with its statement.
                {"SELECT 1;\n-- next\nSELECT 2;", new String[] {"SELECT 1;", "-- next\nSELECT 2;"}},
                // The inline data of an INSERT ends at an empty line.
                {"INSERT INTO t FORMAT CSV\n1,'a;b'\n2,c;\n\nSELECT 1;",
                        new String[] {"INSERT INTO t FORMAT CSV\n1,'a;b'\n2,c;", "SELECT 1;"}},
                {"INSERT INTO t FORMAT Values (1);\nSELECT 1;", new String[] {"INSERT INTO t FORMAT Values (1);", "SELECT 1;"}},
                // The FORMAT of a SELECT is not INSERT data.
                {"SELECT 1 FORMAT TSV;\nSELECT 2;", new String[] {"SELECT 1 FORMAT TSV;", "SELECT 2;"}},
                {"INSERT INTO t SELECT format FROM s;\nSELECT 1;", new String[] {"INSERT INTO t SELECT format FROM s;", "SELECT 1;"}},
                {"INSERT INTO t SELECT x FROM input('x UInt8') FORMAT TSV\n1;\n2\n\nSELECT 1;",
                        new String[] {"INSERT INTO t SELECT x FROM input('x UInt8') FORMAT TSV\n1;\n2", "SELECT 1;"}},
                {"INSERT INTO t SELECT 1 AS format FROM numbers(1);\nSELECT 1;",
                        new String[] {"INSERT INTO t SELECT 1 AS format FROM numbers(1);", "SELECT 1;"}},
                {"INSERT INTO t SELECT format x FROM s;\nSELECT 1;", new String[] {"INSERT INTO t SELECT format x FROM s;", "SELECT 1;"}},
                {"INSERT INTO t SELECT format DIV 2 FROM s;\nSELECT 1;", new String[] {"INSERT INTO t SELECT format DIV 2 FROM s;", "SELECT 1;"}},
                {"INSERT INTO t SELECT x FROM s WHERE format GLOBAL IN (1);\nSELECT 1;",
                        new String[] {"INSERT INTO t SELECT x FROM s WHERE format GLOBAL IN (1);", "SELECT 1;"}},
                {"INSERT INTO t (a) SETTINGS async_insert = 0 FORMAT CSV\n1\n\nSELECT 1;",
                        new String[] {"INSERT INTO t (a) SETTINGS async_insert = 0 FORMAT CSV\n1", "SELECT 1;"}},
                {"SELECT 1;;\nSELECT 2", new String[] {"SELECT 1;", "SELECT 2"}},
        };
    }

    @Test(dataProvider = "scripts")
    public void testSplit(String text, String[] expected) {
        assertEquals(sqls(text), Arrays.asList(expected));
    }

    @Test
    public void testTrailingHint() {
        List<Statement> stmts = StatementSplitter.split(
                "SELECT throwIf(1); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }\nSELECT 2;");
        assertEquals(stmts.size(), 2);
        assertEquals(stmts.get(0).full, "SELECT throwIf(1); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }");
        assertEquals(stmts.get(0).hint.serverErrors, Arrays.asList(395));
        assertFalse(stmts.get(1).hint.expectsError());
    }

    @Test
    public void testHintOnNextLineIsLeading() {
        // clickhouse-client takes only the same-line comment as the trailing comment. A hint on the next line is a
        // leading comment of the next statement, and an error hint in a leading comment is ignored.
        List<Statement> stmts = StatementSplitter.split("SELECT 1;\n-- { serverError 60 }\nSELECT 2;");
        assertEquals(stmts.size(), 2);
        assertFalse(stmts.get(0).hint.expectsError());
        assertFalse(stmts.get(1).hint.expectsError());
    }

    @Test
    public void testHintInsideStatement() {
        List<Statement> stmts = StatementSplitter.split("SELECT x -- { serverError UNKNOWN_IDENTIFIER }\n;");
        assertEquals(stmts.size(), 1);
        assertEquals(stmts.get(0).hint.serverErrors, Arrays.asList(47));
    }

    @Test
    public void testEcho() {
        List<Statement> stmts = StatementSplitter.split("SELECT 1;\n-- { echoOn }\nSELECT 2; -- two\nSELECT 3;\n"
                + "-- { echoOff }\nSELECT 4;\nSELECT 5; -- { echo }\n");
        assertEquals(stmts.size(), 5);
        assertEquals(stmts.stream().map(s -> s.echo).collect(Collectors.toList()),
                Arrays.asList(false, true, true, false, true));
        assertEquals(stmts.get(1).full, "-- { echoOn }\nSELECT 2; -- two");
    }

    @Test
    public void testLeadingKeyword() {
        assertEquals(SqlText.leadingKeyword("  -- c\n/* d */ (select 1)"), "SELECT");
        assertEquals(SqlText.leadingKeyword("insert into t values (1)"), "INSERT");
        assertEquals(SqlText.leadingKeyword("-- only a comment"), "");
    }

    @Test
    public void testKeepsSemicolon() {
        // clickhouse-client sends the semicolon; system.query_log shows it.
        List<Statement> stmts = StatementSplitter.split("SELECT 1; -- c\nSELECT 2\n");
        assertEquals(stmts.size(), 2);
        assertEquals(stmts.get(0).sql, "SELECT 1;");
        assertEquals(stmts.get(1).sql, "SELECT 2");
    }

    @Test
    public void testEchoOfLastStatementWithoutSemicolon() {
        // clickhouse-client echoes the rest of the text, with the newlines.
        List<Statement> stmts = StatementSplitter.split("SELECT 1;\nSELECT 2\n\n");
        assertEquals(stmts.size(), 2);
        assertEquals(stmts.get(0).full, "SELECT 1;");
        assertEquals(stmts.get(1).full, "SELECT 2\n\n");
        assertEquals(stmts.get(1).sql, "SELECT 2");
    }

    @DataProvider
    public Object[][] outputFormats() {
        return new Object[][] {
                {"SELECT 1", "TabSeparated", true},
                {"SELECT 1 FORMAT TSV", "TSV", true},
                {"SELECT 1 FORMAT TSVWithNames SETTINGS max_threads = 1", "TSVWithNames", true},
                {"SELECT 1 FORMAT JSONEachRow", "JSONEachRow", false},
                {"SELECT 'FORMAT JSON'", "TabSeparated", true},
                {"SELECT * FROM (SELECT 1 FORMAT JSON)", "TabSeparated", true},
                {"SELECT t.format FROM t", "TabSeparated", true},
                {"SELECT 1 FORMAT null", "null", true},
                {"SELECT 1 FORMAT TSVRaw", "TSVRaw", true},
                {"SELECT 1 FORMAT `Null`;", "Null", true},
                // FORMAT as an identifier, not the clause.
                {"SELECT format FROM t", "TabSeparated", true},
                {"SELECT x AS format FROM t WHERE format = 1", "TabSeparated", true},
                {"SELECT 1 AS format FROM numbers(1)", "TabSeparated", true},
                {"SELECT format FROM (SELECT 1 AS format)", "TabSeparated", true},
                {"SELECT 1 AS format", "TabSeparated", true},
                {"SELECT 1 AS format ORDER BY format DESC", "TabSeparated", true},
                {"SELECT 1 AS format ORDER BY format ASC", "TabSeparated", true},
                {"SELECT 1 AS format ORDER BY format DESC FORMAT TSV", "TSV", true},
                {"SELECT 1 FORMAT TSV -- c\n;", "TSV", true},
        };
    }

    @Test(dataProvider = "outputFormats")
    public void testOutputFormat(String sql, String name, boolean supported) {
        OutputFormat f = OutputFormat.of(sql);
        assertEquals(f.name, name, sql);
        assertEquals(f.supported, supported, sql);
    }

    @Test
    public void testOutputFormatFlags() {
        OutputFormat f = OutputFormat.of("SELECT 1 FORMAT TabSeparatedRawWithNamesAndTypes");
        assertTrue(f.raw && f.names && f.types && !f.discard);
        assertTrue(OutputFormat.of("SELECT 1 FORMAT Null").discard);
    }
}

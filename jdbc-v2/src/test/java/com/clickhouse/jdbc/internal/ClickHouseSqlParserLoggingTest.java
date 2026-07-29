package com.clickhouse.jdbc.internal;

import com.clickhouse.jdbc.internal.parser.javacc.ClickHouseSqlParser;
import com.clickhouse.jdbc.internal.parser.javacc.ClickHouseSqlStatement;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Verifies that a parse failure does not expose the SQL statement or the parser error detail at
 * {@code WARN}. The statement that failed to parse can carry credentials or PII, so the {@code WARN}
 * stays a generic message; the raw SQL and the parser exception are only acceptable at {@code DEBUG}.
 */
public class ClickHouseSqlParserLoggingTest {

    @Test(groups = { "unit" })
    public void testParseFailureWarnDoesNotExposeSql() throws Exception {
        String secret = "s3cret_db_" + System.nanoTime();
        // A trailing identifier after USE <db> is unexpected, so parsing fails with the token echoed
        // in the parser error message.
        String sql = "USE " + secret + " " + secret;

        // Precondition: the raw parser error embeds the offending SQL token, so logging it (or the
        // SQL) at WARN would expose statement contents.
        String rawError = null;
        try {
            new ClickHouseSqlParser(sql, null).sql();
            fail("expected the invalid SQL to fail parsing");
        } catch (Exception e) {
            rawError = e.getMessage();
        }
        assertNotNull(rawError, "parser error should carry a message");
        assertTrue(rawError.contains(secret), "the parser error is expected to embed the SQL token");

        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream originalErr = System.err;
        System.setErr(new PrintStream(captured, true, "UTF-8"));
        ClickHouseSqlStatement[] stmts;
        try {
            stmts = ClickHouseSqlParser.parse(sql, null);
        } finally {
            System.err.flush();
            System.setErr(originalErr);
        }

        // A parse failure degrades to a single fallback statement rather than throwing.
        assertNotNull(stmts);
        assertTrue(stmts.length >= 1);

        // WARN is always enabled, so the parse-failure warning is emitted here regardless of the
        // DEBUG configuration. Isolate the WARN-level line(s) and assert they neither echo the parser
        // error nor expose the SQL token (both belong only at DEBUG).
        StringBuilder warnLines = new StringBuilder();
        for (String line : captured.toString("UTF-8").split("\\R")) {
            if (line.contains(" WARN ") && line.contains("ClickHouseSqlParser")) {
                warnLines.append(line).append('\n');
            }
        }
        String warn = warnLines.toString();
        assertFalse(warn.isEmpty(), "expected a parse-failure WARN to be emitted");
        assertFalse(warn.contains(secret), "parse-failure WARN exposed the SQL token:\n" + warn);
        assertFalse(warn.contains("Encountered"), "parse-failure WARN echoed the parser error detail:\n" + warn);
    }
}

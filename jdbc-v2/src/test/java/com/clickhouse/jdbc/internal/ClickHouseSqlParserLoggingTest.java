package com.clickhouse.jdbc.internal;

import com.clickhouse.jdbc.internal.parser.javacc.ClickHouseSqlParser;
import com.clickhouse.jdbc.internal.parser.javacc.ClickHouseSqlStatement;
import com.clickhouse.jdbc.LoggingTestUtils;
import org.slf4j.spi.LocationAwareLogger;
import org.testng.annotations.Test;

import java.util.Collections;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Verifies that a parse failure logs nothing at any level: the statement that failed to parse can carry credentials
 * or PII, and the parser error quotes it.
 */
public class ClickHouseSqlParserLoggingTest {

    @Test(groups = { "unit" })
    public void testParseFailureLogsNoSql() throws Exception {
        String secret = "s3cret_db_" + System.nanoTime();
        // A trailing identifier after USE <db> is unexpected, so parsing fails with the token echoed
        // in the parser error message.
        String sql = "USE " + secret + " " + secret;

        // Precondition: the raw parser error embeds the offending SQL token, so logging it (or the
        // SQL) would expose statement contents.
        String rawError = null;
        try {
            new ClickHouseSqlParser(sql, null).sql();
            fail("expected the invalid SQL to fail parsing");
        } catch (Exception e) {
            rawError = e.getMessage();
        }
        assertNotNull(rawError, "parser error should carry a message");
        assertTrue(rawError.contains(secret), "the parser error is expected to embed the SQL token");

        ClickHouseSqlStatement[][] stmts = new ClickHouseSqlStatement[1][];
        String logged = LoggingTestUtils.captureLogs(
                Collections.singletonList(ClickHouseSqlParser.class.getName()), LocationAwareLogger.TRACE_INT,
                () -> stmts[0] = ClickHouseSqlParser.parse(sql, null));

        // A parse failure degrades to a single fallback statement rather than throwing.
        assertNotNull(stmts[0]);
        assertTrue(stmts[0].length >= 1);

        assertTrue(logged.isEmpty(), "a parse failure must not be logged:\n" + logged);
    }
}

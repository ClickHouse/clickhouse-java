package com.clickhouse.jdbc;


import com.clickhouse.client.api.ServerException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Supplier;

@Test(groups = { "integration" })
public class JDBCErrorHandlingTests extends JdbcIntegrationTest {

    @Test(groups = {"integration"})
    public void testServerErrorCodePropagatedToSQLException() throws Exception {
        try (Connection conn = getJdbcConnection(); Statement stmt = conn.createStatement()) {
                stmt.executeQuery("SELECT * FROM somedb.unknown_table");
        } catch (SQLException e) {
            Assert.assertEquals(e.getErrorCode(), ServerException.ErrorCodes.DATABASE_NOT_FOUND.getCode());
        }
    }

    @Test(groups = {"integration"})
    public void testQueryIDPropagatedToException() throws Exception {
        final Queue<String> queryIds = new ConcurrentLinkedQueue<>(); // non-blocking
        final Supplier<String> queryIdGen = () -> {
            String id = UUID.randomUUID().toString();
            queryIds.add(id);
            return id;
        };
        int requests = 3;

        Properties connConfig = new Properties();
        connConfig.put(DriverProperties.QUERY_ID_GENERATOR.getKey(), queryIdGen);
        for (int i = 0; i < requests; i++) {
            try (Connection conn = getJdbcConnection(connConfig); Statement stmt = conn.createStatement()) {
                stmt.executeQuery("SELECT * FROM somedb.unknown_table");
            } catch (SQLException e) {
                Assert.assertEquals(e.getErrorCode(), ServerException.ErrorCodes.DATABASE_NOT_FOUND.getCode());
            }
        }

        Assert.assertEquals(queryIds.size(), requests);
    }

    /**
     * When result-set metadata cannot be resolved because the DESCRIBE fails (here: an unknown table),
     * {@code getMetaData()} must fall back to untyped metadata instead of throwing, and the failure must
     * be logged at WARN with the exception cause attached (the previous log dropped the cause).
     */
    @Test(groups = {"integration"})
    public void testPreparedStatementMetadataFallbackLogsCause() throws Exception {
        String sql = "SELECT c FROM nonexistent_table_for_metadata_logging WHERE id = ?";

        PrintStream original = System.err;
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        ResultSetMetaData md;
        System.setErr(new PrintStream(buf, true, "UTF-8"));
        try (Connection conn = getJdbcConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            md = ps.getMetaData();
        } finally {
            System.setErr(original);
        }
        String logged = buf.toString("UTF-8");

        // The fallback returns metadata rather than throwing or returning null.
        Assert.assertNotNull(md, "metadata must fall back to untyped columns, not be null");

        // The fallback is logged at WARN and now carries the exception cause (the old log omitted it).
        Assert.assertTrue(logged.contains("Failed to resolve result-set metadata"),
                "the metadata fallback must be logged at WARN: " + logged);
        Assert.assertTrue(logged.contains("Exception"),
                "the exception cause must be attached to the log: " + logged);
    }
}

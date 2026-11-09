package com.clickhouse.jdbc;


import com.clickhouse.client.api.ServerException;
import org.slf4j.spi.LocationAwareLogger;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
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
     * be logged at DEBUG with the stack trace of the exception.
     */
    @Test(groups = {"integration"})
    public void testPreparedStatementMetadataFallbackLogsCause() throws Exception {
        String sql = "SELECT c FROM nonexistent_table_for_metadata_logging WHERE id = ?";

        ResultSetMetaData[] md = new ResultSetMetaData[1];
        String logged = LoggingTestUtils.captureLogs(
                Collections.singletonList(PreparedStatementImpl.class.getName()), LocationAwareLogger.DEBUG_INT,
                () -> {
                    try (Connection conn = getJdbcConnection();
                         PreparedStatement ps = conn.prepareStatement(sql)) {
                        md[0] = ps.getMetaData();
                    }
                });

        // The fallback returns metadata rather than throwing or returning null.
        Assert.assertNotNull(md[0], "metadata must fall back to untyped columns, not be null");

        Assert.assertTrue(logged.contains(" DEBUG ") && logged.contains("Failed to resolve result-set metadata"),
                "the metadata fallback must be logged at DEBUG: " + logged);
        Assert.assertTrue(logged.contains("\tat "), "the stack trace of the failure must be logged: " + logged);
    }
}

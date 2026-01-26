package com.clickhouse.jdbc;


import com.clickhouse.client.api.ServerException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.Connection;
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
}

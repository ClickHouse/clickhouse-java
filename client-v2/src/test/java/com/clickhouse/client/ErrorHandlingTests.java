package com.clickhouse.client;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.DataTransferException;
import com.clickhouse.client.api.ServerException;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.query.QuerySettings;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.time.temporal.ChronoUnit;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Test(groups = {"integration"})
public class ErrorHandlingTests extends BaseIntegrationTest {

    static {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "DEBUG");
    }

    /**
     * Tests that a SQL error throws a ServerException.
     */
    @Test(groups = {"integration"})
    void testServerError() throws Exception {
        try (Client client = newClient().build()) {
            // Execute a query against a non-existent table
            client.query("SELECT * FROM non_existent_table_xyz_123").get(10, TimeUnit.SECONDS);
            Assert.fail("Expected ServerException to be thrown");
        } catch (ServerException e) {
            Assert.assertEquals(e.getCode(), ServerException.TABLE_NOT_FOUND);
            Assert.assertFalse(e.getQueryId().isEmpty());
            Assert.assertTrue(e.getMessage().contains(e.getQueryId()));
        }
    }

    /**
     * Tests that a SQL error throws a ServerException when async option is enabled.
     */
    @Test(groups = {"integration"})
    void testServerErrorAsync() throws Exception {
        try (Client client = newClient().useAsyncRequests(true).build()) {
            // Execute a query against a non-existent table
            client.query("SELECT * FROM non_existent_table_xyz_123").get(10, TimeUnit.SECONDS);
            Assert.fail("Expected ServerException to be thrown");
        } catch (ExecutionException e) {
            Assert.assertTrue(e.getCause() instanceof ServerException,
                    "Expected cause to be ServerException but was: " + e.getCause().getClass().getName());
            ServerException se = (ServerException) e.getCause();
            Assert.assertEquals(se.getCode(), ServerException.TABLE_NOT_FOUND,
                    "Expected TABLE_NOT_FOUND error code");
            Assert.assertEquals(se.getCode(), ServerException.TABLE_NOT_FOUND);
            Assert.assertFalse(se.getQueryId().isEmpty());
            Assert.assertTrue(se.getMessage().contains(se.getQueryId()));
        }
    }

    /**
     * Tests that a query exceeding max_execution_time throws a ServerException with TIMEOUT_EXCEEDED code.
     */
    @Test(groups = {"integration"})
    void testQueryTimeout() throws Exception {
        String queryId = "test-query-id";
        try (Client client = newClient().setSocketTimeout(1, ChronoUnit.SECONDS).build()) {
            QuerySettings settings = new QuerySettings().setQueryId(queryId);

            // Execute a query that will take longer than 1 second using sleep function
            client.query("SELECT sleep(3)", settings).get(10, TimeUnit.SECONDS);
            Assert.fail("Expected ServerException to be thrown due to timeout");
        } catch (DataTransferException e) {
            Assert.assertTrue(e.getMessage().contains(queryId));
            Assert.assertEquals(e.getQueryId(), queryId);
        }
    }

    protected Client.Builder newClient() {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        boolean isSecure = isCloud();
        return new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), isSecure)
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .setDefaultDatabase(ClickHouseServerForTest.getDatabase());
    }
}

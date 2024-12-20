package com.clickhouse.client.command;

import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.ServerException;
import com.clickhouse.client.api.command.CommandResponse;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.internal.ServerSettings;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class CommandTests extends BaseIntegrationTest {

    private Client client;

    @BeforeMethod(groups = {"integration"})
    public void setUp() {
        client = newClient().build();
    }


    @Test(groups = {"integration"})
    public void testCreateTable() throws Exception {
        client.execute("DROP TABLE IF EXISTS test_table").get(10, TimeUnit.SECONDS);
        CommandResponse response =
                client.execute("CREATE TABLE IF NOT EXISTS test_table (id UInt32, name String) ENGINE = Memory")
                        .get(10, TimeUnit.SECONDS);

        Assert.assertNotNull(response);
    }

    @Test(groups = {"integration"})
    public void testInvalidCommandExecution() throws Exception {
        try {
            client.execute("ALTER TABLE non_existing_table ADD COLUMN id2 UInt32").get(10, TimeUnit.SECONDS);
        } catch (ServerException e) {
            Assert.assertEquals(e.getCode(), 60);
        } catch (ExecutionException e) {
            Assert.assertTrue(e.getCause() instanceof ServerException);
        } catch (ClientException e) {
            // expected
        }
    }

    protected Client.Builder newClient() {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        boolean isSecure = isCloud();
        return new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), isSecure)
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .compressClientRequest(false)
                .setDefaultDatabase(ClickHouseServerForTest.getDatabase())
                .serverSetting(ServerSettings.WAIT_END_OF_QUERY, "1")
                .useNewImplementation(System.getProperty("client.tests.useNewImplementation", "true").equals("true"));
    }
}

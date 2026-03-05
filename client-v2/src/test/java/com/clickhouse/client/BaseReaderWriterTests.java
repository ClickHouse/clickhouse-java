package com.clickhouse.client;


import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.internal.ServerSettings;
import org.testng.annotations.Test;

@Test(groups = {"integration"})
public class BaseReaderWriterTests extends BaseIntegrationTest {

    private final Client sharedClient;

    protected BaseReaderWriterTests() {
        sharedClient = createSharedClient();
    }


    @Test(groups = {"integration"})
    void doTest() {



    }

    protected Client getSharedClient() {
        return sharedClient;
    }

    protected Client createSharedClient() {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        boolean isSecure = isCloud();
        return new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), isSecure)
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .compressClientRequest(false)
                .setDefaultDatabase(ClickHouseServerForTest.getDatabase())
                .serverSetting(ServerSettings.WAIT_ASYNC_INSERT, "1")
                .serverSetting(ServerSettings.ASYNC_INSERT, "0").build();
    }
}

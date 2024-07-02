package com.clickhouse.client;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.config.ClickHouseClientOption;
import org.junit.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.net.ConnectException;
import java.sql.Connection;
import java.util.Optional;

public class ClientTests extends BaseIntegrationTest {


    @Test(dataProvider = "clientProvider")
    public void testAddSecureEndpoint(Client client) {
        try {
            Optional<GenericRecord> genericRecord = client
                    .queryAll("SELECT hostname()").stream().findFirst();
            Assert.assertTrue(genericRecord.isPresent());
        } catch (ClientException e) {
            e.printStackTrace();
            if (e.getCause().getCause() instanceof ClickHouseException) {
                Exception cause = (Exception) e.getCause().getCause().getCause();
                Assert.assertTrue(cause instanceof ConnectException);
                // TODO: correct when SSL support is fully implemented.
                Assert.assertTrue(cause.getMessage()
                        .startsWith("HTTP request failed: PKIX path building failed"));
                return;
            }
            Assert.fail(e.getMessage());
        }
    }

    @DataProvider(name = "clientProvider")
    private static Client[] secureClientProvider() throws Exception {
        ClickHouseNode node = ClickHouseServerForTest.getClickHouseNode(ClickHouseProtocol.HTTP,
                true, ClickHouseNode.builder()
                                .addOption(ClickHouseClientOption.SSL_MODE.getKey(), "none")
                        .addOption(ClickHouseClientOption.SSL.getKey(), "true").build());
        return new Client[]{
                new Client.Builder()
                        .addEndpoint("https://" + node.getHost() + ":" + node.getPort())
                        .setUsername("default")
                        .setPassword("")
                        .build(),
                new Client.Builder()
                        .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), true)
                        .setUsername("default")
                        .setPassword("")
                        .build()
        };
    }
}

package com.clickhouse.client;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.client.api.query.Records;
import com.clickhouse.client.config.ClickHouseClientOption;
import org.junit.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.net.ConnectException;
import java.sql.Connection;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

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
        } finally {
            client.close();
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


    @Test
    public void testRawSettings() {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        Client client = new Client.Builder()
                .addEndpoint(node.toUri().toString())
                .setUsername("default")
                .setPassword("")
                .setOption("custom_setting_1", "value_1")
                .build();

        client.execute("SELECT 1");

        QuerySettings querySettings = new QuerySettings();
        querySettings.setOption("session_timezone", "Europe/Zurich");

        try (Records response =
                     client.queryRecords("SELECT timeZone(), serverTimeZone()", querySettings).get(10, TimeUnit.SECONDS)) {

            response.forEach(record -> {
                System.out.println(record.getString(1) + " " + record.getString(2));
                Assert.assertEquals("Europe/Zurich", record.getString(1));
                Assert.assertEquals("UTC", record.getString(2));
            });
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        } finally {
            client.close();
        }
    }
}

package com.clickhouse.client;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.client.api.query.Records;
import com.clickhouse.client.config.ClickHouseClientOption;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.net.ConnectException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class ClientTests extends BaseIntegrationTest {

//    static {
//        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "DEBUG");
//    }

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
                        .setRootCertificate("containers/clickhouse-server/certs/localhost.crt")
                        .build(),
                new Client.Builder()
                        .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), true)
                        .setUsername("default")
                        .setPassword("")
                        .setRootCertificate("containers/clickhouse-server/certs/localhost.crt")
                        .build()
        };
    }

    @Test(groups = { "integration" })
    public void testSecureConnection() {
        ClickHouseNode secureServer = getSecureServer(ClickHouseProtocol.HTTP);

        try (Client client = new Client.Builder()
                .addEndpoint("https://localhost:" + secureServer.getPort())
                .setUsername("default")
                .setPassword("")
                .setRootCertificate("containers/clickhouse-server/certs/localhost.crt")
                .useNewImplementation(System.getProperty("client.tests.useNewImplementation", "false").equals("true"))
                .build()) {

            List<GenericRecord> records = client.queryAll("SELECT timezone()");
            Assert.assertTrue(records.size() > 0);
            Assert.assertEquals(records.get(0).getString(1), "UTC");
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
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

    @Test
    public void testPing() {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        try (Client client = new Client.Builder()
                .addEndpoint(node.toUri().toString())
                .setUsername("default")
                .setPassword("")
                .useNewImplementation(System.getProperty("client.tests.useNewImplementation", "false").equals("true"))
                .build()) {
            Assert.assertTrue(client.ping());
        }

        try (Client client = new Client.Builder()
                .addEndpoint("http://localhost:12345")
                .setUsername("default")
                .setPassword("")
                .useNewImplementation(System.getProperty("client.tests.useNewImplementation", "false").equals("true"))
                .build()) {
            Assert.assertFalse(client.ping(TimeUnit.SECONDS.toMillis(20)));
        }
    }
}

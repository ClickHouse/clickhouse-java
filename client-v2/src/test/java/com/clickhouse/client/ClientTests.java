package com.clickhouse.client;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.client.api.query.Records;
import com.clickhouse.client.config.ClickHouseClientOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.net.ConnectException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ClientTests extends BaseIntegrationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClientTests.class);

    @Test(dataProvider = "clientProvider")
    public void testAddSecureEndpoint(Client client) {
        if (isCloud()) {
            return; // will fail in other tests
        }
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
                        .setClientKey("user.key")
                        .setClientCertificate("user.crt")
                        .build()
        };
    }

    @Test
    public void testRawSettings() {
        Client client = newClient()
                .setOption("custom_setting_1", "value_1")
                .build();

        client.execute("SELECT 1");

        QuerySettings querySettings = new QuerySettings();
        querySettings.serverSetting("session_timezone", "Europe/Zurich");

        try (Records response =
                     client.queryRecords("SELECT timeZone(), serverTimeZone()", querySettings).get(10, TimeUnit.SECONDS)) {

            response.forEach(record -> {
                System.out.println(record.getString(1) + " " + record.getString(2));
                Assert.assertEquals("Europe/Zurich", record.getString(1));
                Assert.assertEquals("UTC", record.getString(2));
            });
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            client.close();
        }
    }

    @Test
    public void testPing() {
        try (Client client = newClient().build()) {
            Assert.assertTrue(client.ping());
        }
    }

    @Test
    public void testPingUnpooled() {
        try (Client client = newClient().enableConnectionPool(false).build()) {
            Assert.assertTrue(client.ping());
        }
    }

    @Test
    public void testPingFailure() {
        try (Client client = new Client.Builder()
                .addEndpoint("http://localhost:12345")
                .setUsername("default")
                .setPassword("")
                .useNewImplementation(System.getProperty("client.tests.useNewImplementation", "false").equals("true"))
                .build()) {
            Assert.assertFalse(client.ping(TimeUnit.SECONDS.toMillis(20)));
        }
    }

    @Test
    public void testSetOptions() {
        Map<String, String> options = new HashMap<>();
        String productName = "my product_name (version 1.0)";
        options.put(ClickHouseClientOption.PRODUCT_NAME.getKey(), productName);
        try (Client client = newClient()
                .setOptions(options).build()) {

            Assert.assertEquals(client.getConfiguration().get(ClickHouseClientOption.PRODUCT_NAME.getKey()), productName);
        }
    }

    @Test
    public void testProvidedExecutor() throws Exception {

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        try (Client client = newClient().useAsyncRequests(true).setSharedOperationExecutor(executorService).build()) {
            QueryResponse response = client.query("SELECT 1").get();
            response.getMetrics();
        } catch (Exception e) {
            Assert.fail("unexpected exception", e);
        }

        AtomicBoolean flag = new AtomicBoolean(true);
        executorService.submit(() -> flag.compareAndSet(true, false));
        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.SECONDS);

        Assert.assertFalse(flag.get());
    }

    @Test
    public void testLoadingServerContext() throws Exception {
        long start = System.nanoTime();
        try (Client client = newClient().build()) {
            long initTime = (System.nanoTime() - start) / 1_000_000;
            Assert.assertTrue(initTime < 100);
            Assert.assertNull(client.getServerVersion());
            client.loadServerInfo();
            Assert.assertNotNull(client.getServerVersion());
        }
    }

    @Test
    public void testDisableNative() {
        try (Client client = newClient().disableNativeCompression(true).build()) {
            Assert.assertTrue(client.toString().indexOf("JavaUnsafe") != -1);
        }
    }

    protected Client.Builder newClient() {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        boolean isSecure = isCloud();
        return new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), isSecure)
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .setDefaultDatabase(ClickHouseServerForTest.getDatabase())
                .useNewImplementation(System.getProperty("client.tests.useNewImplementation", "true").equals("true"));
    }
}

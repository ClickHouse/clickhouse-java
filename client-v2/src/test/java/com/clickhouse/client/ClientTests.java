package com.clickhouse.client;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.ClientFaultCause;
import com.clickhouse.client.api.ConnectionReuseStrategy;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.internal.ClickHouseLZ4OutputStream;
import com.clickhouse.client.api.metadata.DefaultColumnToMethodMatchingStrategy;
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

import static java.time.temporal.ChronoUnit.MILLIS;
import static java.time.temporal.ChronoUnit.SECONDS;

public class ClientTests extends BaseIntegrationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClientTests.class);

    @Test(groups = {"integration"}, dataProvider = "secureClientProvider")
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

    @DataProvider
    public static Object[][] secureClientProvider() throws Exception {
        ClickHouseNode node = ClickHouseServerForTest.getClickHouseNode(ClickHouseProtocol.HTTP,
                true, ClickHouseNode.builder()
                                .addOption(ClickHouseClientOption.SSL_MODE.getKey(), "none")
                        .addOption(ClickHouseClientOption.SSL.getKey(), "true").build());
        return new Client[][]{
                {
                        new Client.Builder()
                                .addEndpoint("https://" + node.getHost() + ":" + node.getPort())
                                .setUsername("default")
                                .setPassword("")
                                .setRootCertificate("containers/clickhouse-server/certs/localhost.crt")
                                .build()
                },
                {
                        new Client.Builder()
                                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), true)
                                .setUsername("default")
                                .setPassword("")
                                .setRootCertificate("containers/clickhouse-server/certs/localhost.crt")
                                .setClientKey("user.key")
                                .setClientCertificate("user.crt")
                                .build()
                }
        };
    }

    @Test(groups = {"integration"})
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

    @Test(groups = {"integration"})
    public void testPing() {
        try (Client client = newClient().build()) {
            Assert.assertTrue(client.ping());
        }
    }

    @Test(groups = {"integration"})
    public void testPingUnpooled() {
        try (Client client = newClient().enableConnectionPool(false).build()) {
            Assert.assertTrue(client.ping());
        }
    }

    @Test(groups = {"integration"})
    public void testPingFailure() {
        try (Client client = new Client.Builder()
                .addEndpoint("http://localhost:12345")
                .setUsername("default")
                .setPassword("")
                .build()) {
            Assert.assertFalse(client.ping(TimeUnit.SECONDS.toMillis(20)));
        }
    }

    @Test(groups = {"integration"})
    public void testPingAsync() {
        try (Client client = newClient().useAsyncRequests(true).build()) {
            Assert.assertTrue(client.ping());
        }
    }

    @Test(groups = {"integration"})
    public void testSetOptions() {
        Map<String, String> options = new HashMap<>();
        String productName = "my product_name (version 1.0)";
        options.put(ClickHouseClientOption.PRODUCT_NAME.getKey(), productName);
        try (Client client = newClient()
                .setOptions(options).build()) {

            Assert.assertEquals(client.getConfiguration().get(ClickHouseClientOption.PRODUCT_NAME.getKey()), productName);
        }
    }

    @Test(groups = {"integration"})
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

    @Test(groups = {"integration"})
    public void testLoadingServerContext() throws Exception {
        long start = System.nanoTime();
        try (Client client = newClient().build()) {
            long initTime = (System.nanoTime() - start) / 1_000_000;
            Assert.assertTrue(initTime < 100);
            Assert.assertEquals(client.getServerVersion(), "unknown");
            client.loadServerInfo();
            Assert.assertNotNull(client.getServerVersion());
        }
    }

    @Test(groups = {"integration"})
    public void testDisableNative() {
        try (Client client = newClient().disableNativeCompression(true).build()) {
            Assert.assertTrue(client.toString().indexOf("JavaUnsafe") != -1);
        }
    }

    @Test(groups = {"integration"})
    public void testDefaultSettings() {
        try (Client client = new Client.Builder().setUsername("default").setPassword("secret")
                .addEndpoint("http://localhost:8123").build()) {
            Map<String, String> config = client.getConfiguration();
            for (ClientConfigProperties p : ClientConfigProperties.values()) {
                if (p.getDefaultValue() != null) {
                    Assert.assertTrue(config.containsKey(p.getKey()), "Default value should be set for " + p.getKey());
                    Assert.assertEquals(config.get(p.getKey()), p.getDefaultValue(), "Default value doesn't match");
                }
            }
            Assert.assertEquals(config.size(), 31); // to check everything is set. Increment when new added.
        }

        try (Client client = new Client.Builder()
                .setUsername("default")
                .setPassword("secret")
                .addEndpoint("http://localhost:8123")
                .setDefaultDatabase("mydb")
                .setExecutionTimeout(10, MILLIS)
                .setLZ4UncompressedBufferSize(300_000)
                .disableNativeCompression(true)
                .useServerTimeZone(false)
                .setServerTimeZone("America/Los_Angeles")
                .useTimeZone("America/Los_Angeles")
                .useAsyncRequests(true)
                .setMaxConnections(330)
                .setConnectionRequestTimeout(20, SECONDS)
                .setConnectionReuseStrategy(ConnectionReuseStrategy.LIFO)
                .enableConnectionPool(false)
                .setConnectionTTL(30, SECONDS)
                .retryOnFailures(ClientFaultCause.NoHttpResponse)
                .setClientNetworkBufferSize(500_000)
                .setMaxRetries(10)
                .useHTTPBasicAuth(false)
                .compressClientRequest(true)
                .compressServerResponse(false)
                .useHttpCompression(true)
                .appCompressedData(true)
                .setSocketTimeout(20, SECONDS)
                .setSocketRcvbuf(100000)
                .setSocketSndbuf(100000)
                .build()) {
            Map<String, String> config = client.getConfiguration();
            Assert.assertEquals(config.size(), 32); // to check everything is set. Increment when new added.
            Assert.assertEquals(config.get(ClientConfigProperties.DATABASE.getKey()), "mydb");
            Assert.assertEquals(config.get(ClientConfigProperties.MAX_EXECUTION_TIME.getKey()), "10");
            Assert.assertEquals(config.get(ClientConfigProperties.COMPRESSION_LZ4_UNCOMPRESSED_BUF_SIZE.getKey()), "300000");
            Assert.assertEquals(config.get(ClientConfigProperties.DISABLE_NATIVE_COMPRESSION.getKey()), "true");
            Assert.assertEquals(config.get(ClientConfigProperties.USE_SERVER_TIMEZONE.getKey()), "false");
            Assert.assertEquals(config.get(ClientConfigProperties.SERVER_TIMEZONE.getKey()), "America/Los_Angeles");
            Assert.assertEquals(config.get(ClientConfigProperties.ASYNC_OPERATIONS.getKey()), "true");
            Assert.assertEquals(config.get(ClientConfigProperties.HTTP_MAX_OPEN_CONNECTIONS.getKey()), "330");
            Assert.assertEquals(config.get(ClientConfigProperties.CONNECTION_REQUEST_TIMEOUT.getKey()), "20000");
            Assert.assertEquals(config.get(ClientConfigProperties.CONNECTION_REUSE_STRATEGY.getKey()), "LIFO");
            Assert.assertEquals(config.get(ClientConfigProperties.CONNECTION_POOL_ENABLED.getKey()), "false");
            Assert.assertEquals(config.get(ClientConfigProperties.CONNECTION_TTL.getKey()), "30000");
            Assert.assertEquals(config.get(ClientConfigProperties.CLIENT_RETRY_ON_FAILURE.getKey()), "NoHttpResponse");
            Assert.assertEquals(config.get(ClientConfigProperties.CLIENT_NETWORK_BUFFER_SIZE.getKey()), "500000");
            Assert.assertEquals(config.get(ClientConfigProperties.RETRY_ON_FAILURE.getKey()), "10");
            Assert.assertEquals(config.get(ClientConfigProperties.HTTP_USE_BASIC_AUTH.getKey()), "false");
            Assert.assertEquals(config.get(ClientConfigProperties.COMPRESS_CLIENT_REQUEST.getKey()), "true");
            Assert.assertEquals(config.get(ClientConfigProperties.COMPRESS_SERVER_RESPONSE.getKey()), "false");
            Assert.assertEquals(config.get(ClientConfigProperties.USE_HTTP_COMPRESSION.getKey()), "true");
            Assert.assertEquals(config.get(ClientConfigProperties.APP_COMPRESSED_DATA.getKey()), "true");
            Assert.assertEquals(config.get(ClientConfigProperties.SOCKET_OPERATION_TIMEOUT.getKey()), "20000");
            Assert.assertEquals(config.get(ClientConfigProperties.SOCKET_RCVBUF_OPT.getKey()), "100000");
            Assert.assertEquals(config.get(ClientConfigProperties.SOCKET_SNDBUF_OPT.getKey()), "100000");
        }
    }

    @Test(groups = {"integration"})
    public void testWithOldDefaults() {
        try (Client client = new Client.Builder()
                .setUsername("default")
                .setPassword("seceret")
                .addEndpoint("http://localhost:8123")
                .setDefaultDatabase("default")
                .setExecutionTimeout(0, MILLIS)
                .setLZ4UncompressedBufferSize(ClickHouseLZ4OutputStream.UNCOMPRESSED_BUFF_SIZE)
                .disableNativeCompression(false)
                .useServerTimeZone(true)
                .setServerTimeZone("UTC")
                .useAsyncRequests(false)
                .setMaxConnections(10)
                .setConnectionRequestTimeout(10, SECONDS)
                .setConnectionReuseStrategy(ConnectionReuseStrategy.FIFO)
                .enableConnectionPool(true)
                .setConnectionTTL(-1, MILLIS)
                .retryOnFailures(ClientFaultCause.NoHttpResponse, ClientFaultCause.ConnectTimeout,
                        ClientFaultCause.ConnectionRequestTimeout, ClientFaultCause.ServerRetryable)
                .setClientNetworkBufferSize(300_000)
                .setMaxRetries(3)
                .allowBinaryReaderToReuseBuffers(false)
                .columnToMethodMatchingStrategy(DefaultColumnToMethodMatchingStrategy.INSTANCE)
                .useHTTPBasicAuth(true)
                .compressClientRequest(false)
                .compressServerResponse(true)
                .useHttpCompression(false)
                .appCompressedData(false)
                .setSocketTimeout(0, SECONDS)
                .setSocketRcvbuf(804800)
                .setSocketSndbuf(804800)
                .build()) {
            Map<String, String> config = client.getConfiguration();
            for (ClientConfigProperties p : ClientConfigProperties.values()) {
                if (p.getDefaultValue() != null) {
                    Assert.assertTrue(config.containsKey(p.getKey()), "Default value should be set for " + p.getKey());
                    Assert.assertEquals(config.get(p.getKey()), p.getDefaultValue(), "Default value doesn't match");
                }
            }
            Assert.assertEquals(config.size(), 31); // to check everything is set. Increment when new added.
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

package com.clickhouse.client;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.ClientFaultCause;
import com.clickhouse.client.api.ClientMisconfigurationException;
import com.clickhouse.client.api.ConnectionReuseStrategy;
import com.clickhouse.client.api.ServerException;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.internal.ClickHouseLZ4OutputStream;
import com.clickhouse.client.api.internal.ServerSettings;
import com.clickhouse.client.api.internal.ValidationUtils;
import com.clickhouse.client.api.metadata.DefaultColumnToMethodMatchingStrategy;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.client.api.query.Records;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseVersion;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.testng.util.Strings;

import java.io.ByteArrayInputStream;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static java.time.temporal.ChronoUnit.MILLIS;
import static java.time.temporal.ChronoUnit.SECONDS;
import static org.testng.AssertJUnit.fail;

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

    @DataProvider(name = "secureClientProvider")
    public static Object[][] secureClientProvider() throws Exception {
        ClickHouseNode node = ClickHouseServerForTest.getClickHouseNode(ClickHouseProtocol.HTTP,
                true, ClickHouseNode.builder()
                                .addOption(ClickHouseClientOption.SSL_MODE.getKey(), "none")
                        .addOption(ClickHouseClientOption.SSL.getKey(), "true").build());

        Client client1;
        Client client2;
        try {
            client1 = new Client.Builder()
                    .addEndpoint("https://" + node.getHost() + ":" + node.getPort())
                    .setUsername("default")
                    .setPassword(ClickHouseServerForTest.getPassword())
                    .setRootCertificate("containers/clickhouse-server/certs/localhost.crt")
                    .build();

            client2 =                         new Client.Builder()
                    .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), true)
                    .setUsername("default")
                    .setPassword(ClickHouseServerForTest.getPassword())
                    .setRootCertificate("containers/clickhouse-server/certs/localhost.crt")
                    .setClientKey("some_user.key")
                    .setClientCertificate("some_user.crt")
                    .build();

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }


        return new Client[][]{
                {
                     client1
                },
                {
                    client2
                }
        };
    }

    @Test(groups = {"integration"})
    public void testRawSettings() {
        try (Client client = newClient()
                .setOption("custom_setting_1", "value_1")
                .build()) {

            client.execute("SELECT 1");

            QuerySettings querySettings = new QuerySettings();
            querySettings.serverSetting("session_timezone", "Europe/Zurich");

            try (Records response =
                         client.queryRecords("SELECT timeZone(), serverTimeZone()", querySettings).get(10, TimeUnit.SECONDS)) {

                response.forEach(record -> {
                    Assert.assertEquals("Europe/Zurich", record.getString(1));
                    Assert.assertEquals("UTC", record.getString(2));
                });
            } catch (Exception e) {
                Assert.fail(e.getMessage());
            }
        }
    }

    @Test(groups = {"integration"})
    public void testCustomSettings() {
        if (isCloud()) {
            return; // no custom parameters on cloud instance
        }
        final String CLIENT_OPTION = "custom_client_option"; // prefix should be known from server config
        try (Client client = newClient().serverSetting(CLIENT_OPTION, "opt1").build()) {

            final List<GenericRecord> clientOption = client.queryAll("SELECT getSetting({option_name:String})",
                    Collections.singletonMap("option_name", CLIENT_OPTION));

            Assert.assertEquals(clientOption.get(0).getString(1), "opt1");

            QuerySettings querySettings = new QuerySettings();
            querySettings.serverSetting(CLIENT_OPTION, "opt2");

            final List<GenericRecord> requestOption = client.queryAll("SELECT getSetting({option_name:String})",
                    Collections.singletonMap("option_name", CLIENT_OPTION), querySettings);

            Assert.assertEquals(requestOption.get(0).getString(1), "opt2");
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
            Assert.assertTrue(client.toString().contains("JavaSafe") || client.toString().contains("JavaUnsafe"));
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
            Assert.assertEquals(config.size(), 34); // to check everything is set. Increment when new added.
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
            Assert.assertEquals(config.size(), 35); // to check everything is set. Increment when new added.
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
            Assert.assertEquals(config.size(), 34); // to check everything is set. Increment when new added.
        }
    }

    @DataProvider(name = "sessionRoles")
    private static Object[][] sessionRoles() {
        return new Object[][]{
                {new String[]{"ROL1", "ROL2"}},
                {new String[]{"ROL1", "ROL2"}},
                {new String[]{"ROL1", "ROL2"}},
                {new String[]{"ROL1", "ROL2,☺"}},
                {new String[]{"ROL1", "ROL2"}},
        };
    }

    @Test(groups = {"integration"}, dataProvider = "sessionRoles")
    public void testOperationCustomRoles(String[] roles) throws Exception {
        if (isVersionMatch("(,24.3]", newClient().build())) {
            return;
        }

        String password = "^1A" + RandomStringUtils.random(12, true, true) + "3b$";
        final String rolesList = "\"" + Strings.join("\",\"", roles) + "\"";
        try (Client client = newClient().build()) {
            client.execute("DROP ROLE IF EXISTS " + rolesList).get().close();
            client.execute("CREATE ROLE " + rolesList).get().close();
            client.execute("DROP USER IF EXISTS some_user").get().close();
            client.execute("CREATE USER some_user IDENTIFIED BY '" + password + "'").get().close();
            client.execute("GRANT " + rolesList + " TO some_user").get().close();
        }

        try (Client userClient = newClient().setUsername("some_user").setPassword(password).build()) {
            QuerySettings settings = new QuerySettings().setDBRoles(Arrays.asList(roles));
            List<GenericRecord> resp = userClient.queryAll("SELECT currentRoles()", settings);
            Set<String> roleSet = new HashSet<>(Arrays.asList(roles));
            Set<String> currentRoles = new  HashSet<String> (resp.get(0).getList(1));
            Assert.assertEquals(currentRoles, roleSet, "Roles " + roleSet + " not found in " + currentRoles);
        }
    }

    @DataProvider(name = "clientSessionRoles")
    private static Object[][] clientSessionRoles() {
        return new Object[][]{
                {new String[]{"ROL1", "ROL2"}},
                {new String[]{"ROL1", "ROL2,☺"}},
        };
    }
    @Test(groups = {"integration"}, dataProvider = "clientSessionRoles")
    public void testClientCustomRoles(String[] roles) throws Exception {
        if (isVersionMatch("(,24.3]", newClient().build())) {
            return;
        }

        String password = "^1A" + RandomStringUtils.random(12, true, true) + "3B$";
        final String rolesList = "\"" + Strings.join("\",\"", roles) + "\"";
        try (Client client = newClient().build()) {
            client.execute("DROP ROLE IF EXISTS " + rolesList).get().close();
            client.execute("CREATE ROLE " + rolesList).get().close();
            client.execute("DROP USER IF EXISTS some_user").get().close();
            client.execute("CREATE USER some_user IDENTIFIED WITH sha256_password BY '" + password + "'").get().close();
            client.execute("GRANT " + rolesList + " TO some_user").get().close();
        }

        try (Client userClient = newClient().setUsername("some_user").setPassword(password).build()) {
            userClient.setDBRoles(Arrays.asList(roles));
            List<GenericRecord> resp = userClient.queryAll("SELECT currentRoles()");
            Set<String> roleSet = new HashSet<>(Arrays.asList(roles));
            Set<String> currentRoles = new  HashSet<String> (resp.get(0).getList(1));
            Assert.assertEquals(currentRoles, roleSet, "Roles " + roleSet + " not found in " + currentRoles);
        }
    }


    @Test(groups = {"integration"})
    public void testLogComment() throws Exception {

        String logComment = "Test log comment";
        QuerySettings settings = new QuerySettings()
                .setQueryId(UUID.randomUUID().toString())
                .logComment(logComment);

        try (Client client = newClient().build()) {

            try (QueryResponse response = client.query("SELECT 1", settings).get()) {
                Assert.assertNotNull(response.getQueryId());
                Assert.assertTrue(response.getQueryId().startsWith(settings.getQueryId()));
            }

            client.execute("SYSTEM FLUSH LOGS").get().close();

            List<GenericRecord> logRecords = client.queryAll("SELECT query_id, log_comment FROM clusterAllReplicas('default', system.query_log) WHERE query_id = '" + settings.getQueryId() + "'");
            Assert.assertEquals(logRecords.get(0).getString("query_id"), settings.getQueryId());
            Assert.assertEquals(logRecords.get(0).getString("log_comment"), logComment);
        }
    }

    @Test(groups = {"integration"})
    public void testServerSettings() throws Exception {
        try (Client client = newClient().build()) {
            client.execute("DROP TABLE IF EXISTS server_settings_test_table");
            client.execute("CREATE TABLE server_settings_test_table (v Float) Engine MergeTree ORDER BY ()");

            final String queryId = UUID.randomUUID().toString();
            InsertSettings insertSettings = new InsertSettings()
                    .setQueryId(queryId)
                    .serverSetting(ServerSettings.ASYNC_INSERT, "1")
                    .serverSetting(ServerSettings.WAIT_ASYNC_INSERT, "1");

            String csvData = "0.33\n0.44\n0.55\n";
            client.insert("server_settings_test_table", new ByteArrayInputStream(csvData.getBytes()), ClickHouseFormat.CSV, insertSettings).get().close();

            client.execute("SYSTEM FLUSH LOGS").get().close();

            List<GenericRecord> logRecords = client.queryAll("SELECT * FROM clusterAllReplicas('default', system.query_log) WHERE query_id = '" + queryId + "' AND type = 'QueryFinish'");

            GenericRecord record = logRecords.get(0);
            String settings = record.getString(record.getSchema().nameToColumnIndex("Settings"));
            Assert.assertTrue(settings.contains(ServerSettings.ASYNC_INSERT + "=1"));
//            Assert.assertTrue(settings.contains(ServerSettings.WAIT_ASYNC_INSERT + "=1")); // uncomment after server fix 
        }
    }

    @Test(groups = {"integration"})
    public void testInvalidEndpoint() {

        try {
            new Client.Builder().addEndpoint("http://localhost/default");
            fail("Exception expected");
        } catch (ValidationUtils.SettingsValidationException e) {
            Assert.assertTrue(e.getMessage().contains("port"));
        }
    }

    @Test(groups = {"integration"})
    public void testUnknownClientSettings() throws Exception {
        try (Client client = newClient().setOption("unknown_setting", "value").build()) {
            Assert.fail("Exception expected");
        } catch (Exception ex) {
            Assert.assertTrue(ex instanceof ClientMisconfigurationException);
            Assert.assertTrue(ex.getMessage().contains("unknown_setting"));
        }

        try (Client client = newClient().setOption(ClientConfigProperties.NO_THROW_ON_UNKNOWN_CONFIG, "what ever").setOption("unknown_setting", "value").build()) {
            Assert.assertTrue(client.ping());
        }

        try (Client client = newClient().setOption(ClientConfigProperties.SERVER_SETTING_PREFIX + "unknown_setting", "value").build()) {
            try {
                client.execute("SELECT 1");
                Assert.fail("Exception expected");
            } catch (ServerException e) {
                Assert.assertEquals(e.getCode(), ServerException.UNKNOWN_SETTING);
            }
        }

        try (Client client = newClient().setOption(ClientConfigProperties.HTTP_HEADER_PREFIX + "unknown_setting", "value").build()) {
            Assert.assertTrue(client.ping());
        }
    }

    @Test(groups = {"integration"})
    public void testInvalidConfig() {
        try {
            newClient().setOption(ClientConfigProperties.CUSTOM_SETTINGS_PREFIX.getKey(), "").build();
            Assert.fail("exception expected");
        } catch (ClientException e) {
            Assert.assertTrue(e.getMessage().contains(ClientConfigProperties.CUSTOM_SETTINGS_PREFIX.getKey()));
        }
    }

    @Test(groups = {"integration"})
    public void testQueryIdGenerator() throws Exception {
        final String queryId = UUID.randomUUID().toString();
        Supplier<String> constantQueryIdSupplier = () -> queryId;

        // check getting same UUID
        for (int i = 0; i < 3; i++ ) {
            try (Client client = newClient().queryIdGenerator(constantQueryIdSupplier).build()) {
                client.execute("SELECT * FROM unknown_table").get().close();
            } catch (ServerException ex) {
                Assert.assertEquals(ex.getCode(), ServerException.ErrorCodes.TABLE_NOT_FOUND.getCode());
                Assert.assertEquals(ex.getQueryId(), queryId);
            }
        }

        final Queue<String> queryIds = new ConcurrentLinkedQueue<>(); // non-blocking
        final Supplier<String> queryIdGen = () -> {
            String id = UUID.randomUUID().toString();
            queryIds.add(id);
            return id;
        };
        int requests = 3;
        final Queue<String> actualIds = new ConcurrentLinkedQueue<>();
        for (int i = 0; i < requests; i++ ) {
            try (Client client = newClient().queryIdGenerator(queryIdGen).build()) {
                client.execute("SELECT * FROM unknown_table").get().close();
            } catch (ServerException ex) {
                Assert.assertEquals(ex.getCode(), ServerException.ErrorCodes.TABLE_NOT_FOUND.getCode());
                actualIds.add(ex.getQueryId());
            }
        }

        Assert.assertEquals(queryIds.size(), requests);
        Assert.assertEquals(actualIds, new ArrayList<>(queryIds));
    }

    public boolean isVersionMatch(String versionExpression, Client client) {
        List<GenericRecord> serverVersion = client.queryAll("SELECT version()");
        return ClickHouseVersion.of(serverVersion.get(0).getString(1)).check(versionExpression);
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

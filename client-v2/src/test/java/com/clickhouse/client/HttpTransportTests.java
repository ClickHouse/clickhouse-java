package com.clickhouse.client;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.ClientFaultCause;
import com.clickhouse.client.api.ConnectionInitiationException;
import com.clickhouse.client.api.ConnectionReuseStrategy;
import com.clickhouse.client.api.ServerException;
import com.clickhouse.client.api.command.CommandResponse;
import com.clickhouse.client.api.command.CommandSettings;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.enums.ProxyType;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.internal.ServerSettings;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.insert.SamplePOJO;
import com.clickhouse.data.ClickHouseFormat;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.ConsoleNotifier;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.http.Fault;
import com.github.tomakehurst.wiremock.http.trafficlistener.WiremockNetworkTrafficListener;
import org.apache.hc.core5.http.ConnectionRequestTimeoutException;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.net.URIBuilder;
import org.eclipse.jetty.server.Server;
import org.testcontainers.utility.ThrowingFunction;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.function.Supplier;

import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

public class HttpTransportTests extends BaseIntegrationTest {

    @Test(groups = {"integration"},dataProvider = "testConnectionTTLProvider")
    @SuppressWarnings("java:S2925")
    public void testConnectionTTL(Long connectionTtl, Long keepAlive, int openSockets) throws Exception {
        if (isCloud()) {
            return; // skip cloud tests because of wiremock proxy. TODO: fix it
        }
        ClickHouseNode server = getServer(ClickHouseProtocol.HTTP);

        int proxyPort = new Random().nextInt(1000) + 10000;
        ConnectionCounterListener connectionCounter = new ConnectionCounterListener();
        WireMockServer proxy = new WireMockServer(WireMockConfiguration
                .options().port(proxyPort)
                .networkTrafficListener(connectionCounter)
                .notifier(new Slf4jNotifier(true)));
        proxy.start();
        URIBuilder targetURI = new URIBuilder(server.getBaseUri())
                .setPath("");
        proxy.addStubMapping(WireMock.post(WireMock.anyUrl())
                .willReturn(WireMock.aResponse().proxiedFrom(targetURI.build().toString())).build());

        Client.Builder clientBuilder = new Client.Builder()
                .addEndpoint(server.getBaseUri())
                .setUsername("default")
                .setPassword(getPassword())
                .useNewImplementation(true)
                .addProxy(ProxyType.HTTP, "localhost", proxyPort);
        if (connectionTtl != null) {
            clientBuilder.setConnectionTTL(connectionTtl, ChronoUnit.MILLIS);
        }
        if (keepAlive != null) {
            clientBuilder.setKeepAliveTimeout(keepAlive, ChronoUnit.MILLIS);
        }

        try (Client client = clientBuilder.build()) {
            List<GenericRecord> resp = client.queryAll("select 1");
            Assert.assertEquals(resp.stream().findFirst().get().getString(1), "1");

            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                Assert.fail("Unexpected exception", e);
            }

            resp = client.queryAll("select 1");
            Assert.assertEquals(resp.stream().findFirst().get().getString(1), "1");
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception", e);
        } finally {
            Assert.assertEquals(connectionCounter.opened.get(), openSockets);
            proxy.stop();
        }
    }

    @DataProvider(name = "testConnectionTTLProvider")
    public static Object[][]  testConnectionTTLProvider() {
        return new Object[][] {
                { 1000L, null, 2 },
                { 2000L, null, 1 },
                { null, 2000L, 1 },
                { null, 500L, 2 },
                { 1000L, 0L, 2 },
                { 1000L, 3000L, 2}
        };
    }

    private static class ConnectionCounterListener implements WiremockNetworkTrafficListener {

        private AtomicInteger opened = new AtomicInteger(0);
        private AtomicInteger closed = new AtomicInteger(0);

        @Override
        public void opened(Socket socket) {
            opened.incrementAndGet();
        }

        @Override
        public void incoming(Socket socket, ByteBuffer bytes) {
            // ignore
        }

        @Override
        public void outgoing(Socket socket, ByteBuffer bytes) {
            // ignore
        }

        @Override
        public void closed(Socket socket) {
            closed.incrementAndGet();
        }
    }

    @Test(groups = {"integration"})
    public void testConnectionRequestTimeout() {
        if (isCloud()) {
            return; // mocked server
        }

        int serverPort = new Random().nextInt(1000) + 10000;
        ConnectionCounterListener connectionCounter = new ConnectionCounterListener();
        WireMockServer proxy = new WireMockServer(WireMockConfiguration
                .options().port(serverPort)
                .networkTrafficListener(connectionCounter)
                .notifier(new Slf4jNotifier(true)));
        proxy.start();
        proxy.addStubMapping(WireMock.post(WireMock.anyUrl())
                .willReturn(WireMock.aResponse().withFixedDelay(5000)
                        .withStatus(HttpStatus.SC_NOT_FOUND)).build());

        Client.Builder clientBuilder = new Client.Builder()
                .addEndpoint("http://localhost:" + serverPort)
                .setUsername("default")
                .setPassword(getPassword())
                .retryOnFailures(ClientFaultCause.None)
                .useNewImplementation(true)
                .setMaxConnections(1)
                .setOption(ClickHouseClientOption.ASYNC.getKey(), "true")
                .setSocketTimeout(10000, ChronoUnit.MILLIS)
                .setConnectionRequestTimeout(5, ChronoUnit.MILLIS);

        try (Client client = clientBuilder.build()) {
            CompletableFuture<QueryResponse> f1 = client.query("select 1");
            Thread.sleep(500L);
            CompletableFuture<QueryResponse> f2 = client.query("select 1");
            f2.get();
        } catch (ExecutionException e) {
            e.printStackTrace();
            Assert.assertEquals(e.getCause().getClass(), ConnectionInitiationException.class);
            Assert.assertEquals(e.getCause().getCause().getClass(), ConnectionRequestTimeoutException.class);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception", e);
        } finally {
            proxy.stop();
        }
    }

    @Test
    public void testConnectionReuseStrategy() {
        if (isCloud()) {
            return; // mocked server
        }

        ClickHouseNode server = getServer(ClickHouseProtocol.HTTP);

        try (Client client = new Client.Builder()
                .addEndpoint(server.getBaseUri())
                .setUsername("default")
                .setPassword(getPassword())
                .useNewImplementation(true)
                .setConnectionReuseStrategy(ConnectionReuseStrategy.LIFO)
                .build()) {

            List<GenericRecord> records = client.queryAll("SELECT timezone()");
            Assert.assertTrue(records.size() > 0);
            Assert.assertEquals(records.get(0).getString(1), "UTC");
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test(groups = { "integration" })
    public void testSecureConnection() {
        if (isCloud()) {
            return; // will fail in other tests
        }

        ClickHouseNode secureServer = getSecureServer(ClickHouseProtocol.HTTP);

        try (Client client = new Client.Builder()
                .addEndpoint("https://localhost:" + secureServer.getPort())
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .setRootCertificate("containers/clickhouse-server/certs/localhost.crt")
                .compressClientRequest(true)
                .build()) {

            List<GenericRecord> records = client.queryAll("SELECT timezone()");
            Assert.assertTrue(records.size() > 0);
            Assert.assertEquals(records.get(0).getString(1), "UTC");
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test(groups = { "integration" }, dataProvider = "NoResponseFailureProvider")
    public void testInsertAndNoHttpResponseFailure(String body, int maxRetries, ThrowingFunction<Client, Void> function,
                                                   boolean shouldFail) {
        if (isCloud()) {
            return; // mocked server
        }

        WireMockServer faultyServer = new WireMockServer( WireMockConfiguration
                .options().port(9090).notifier(new ConsoleNotifier(false)));
        faultyServer.start();

        // First request gets no response
        faultyServer.addStubMapping(WireMock.post(WireMock.anyUrl())
                .withRequestBody(WireMock.equalTo(body))
                .inScenario("Retry")
                .whenScenarioStateIs(STARTED)
                .willSetStateTo("Failed")
                .willReturn(WireMock.aResponse().withFault(Fault.EMPTY_RESPONSE)).build());

        // Second request gets a response (retry)
        faultyServer.addStubMapping(WireMock.post(WireMock.anyUrl())
                .withRequestBody(WireMock.equalTo(body))
                .inScenario("Retry")
                .whenScenarioStateIs("Failed")
                .willSetStateTo("Done")
                .willReturn(WireMock.aResponse()
                        .withHeader("X-ClickHouse-Summary",
                                "{ \"read_bytes\": \"10\", \"read_rows\": \"1\"}")).build());

        Client mockServerClient = new Client.Builder()
                .addEndpoint(Protocol.HTTP, "localhost", faultyServer.port(), false)
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .useNewImplementation(true) // because of the internal differences
                .compressClientRequest(false)
                .setMaxRetries(maxRetries)
                .build();

        try {
            function.apply(mockServerClient);
        } catch (ClientException e) {
            e.printStackTrace();
            if (!shouldFail) {
                Assert.fail("Unexpected exception", e);
            }
            return;
        } catch (Exception e) {
            Assert.fail("Unexpected exception", e);
        } finally {
            faultyServer.stop();
        }

        if (shouldFail) {
            Assert.fail("Expected exception");
        }
    }

    @DataProvider(name = "NoResponseFailureProvider")
    public static Object[][] noResponseFailureProvider() {

        String insertBody = "1\t2\t3\n";
        ThrowingFunction<Client, Void> insertFunction = (client) -> {
            InsertResponse insertResponse = client.insert("table01",
                    new ByteArrayInputStream("1\t2\t3\n".getBytes()), ClickHouseFormat.TSV).get(30, TimeUnit.SECONDS);
            insertResponse.close();
            return null;
        };

        String selectBody = "select timezone()";
        ThrowingFunction<Client, Void> queryFunction = (client) -> {
            QueryResponse response = client.query("select timezone()").get(30, TimeUnit.SECONDS);
            response.close();
            return null;
        };

        return new Object[][]{
                {insertBody, 1, insertFunction, false},
                {selectBody, 1, queryFunction, false},
                {insertBody, 0, insertFunction, true},
                {selectBody, 0, queryFunction, true}
        };
    }

    @Test(groups = { "integration" }, dataProvider = "testServerErrorHandlingDataProvider")
    public void testServerErrorHandling(ClickHouseFormat format, boolean serverCompression, boolean useHttpCompression) {
        if (isCloud()) {
            return; // mocked server
        }

        ClickHouseNode server = getServer(ClickHouseProtocol.HTTP);
        try (Client client = new Client.Builder()
                .addEndpoint(server.getBaseUri())
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .compressServerResponse(serverCompression)
                .useHttpCompression(useHttpCompression)
                .build()) {

            QuerySettings querySettings = new QuerySettings().setFormat(format);
            try (QueryResponse response =
                         client.query("SELECT invalid;statement", querySettings).get(1, TimeUnit.SECONDS)) {
                Assert.fail("Expected exception");
            } catch (ServerException e) {
                e.printStackTrace();
                Assert.assertEquals(e.getCode(), 62);
                Assert.assertTrue(e.getMessage().startsWith("Code: 62. DB::Exception: Syntax error (Multi-statements are not allowed): failed at position 15 (end of query)"),
                        "Unexpected error message: " + e.getMessage());
            }


            try (QueryResponse response = client.query("CREATE TABLE table_from_csv ENGINE MergeTree ORDER BY () AS SELECT * FROM file('empty.csv') ", querySettings)
                    .get(1, TimeUnit.SECONDS)) {
                Assert.fail("Expected exception");
            } catch (ServerException e) {
                e.printStackTrace();
                Assert.assertEquals(e.getCode(), 636);
                Assert.assertTrue(e.getMessage().contains("You can specify the structure manually: (in file/uri /var/lib/clickhouse/user_files/empty.csv). (CANNOT_EXTRACT_TABLE_STRUCTURE)"),
                        "Unexpected error message: " + e.getMessage());
            }

            querySettings.serverSetting("unknown_setting", "1");
            try (QueryResponse response = client.query("CREATE TABLE table_from_csv AS SELECT * FROM file('empty.csv')", querySettings)
                    .get(1, TimeUnit.SECONDS)) {
                Assert.fail("Expected exception");
            } catch (ServerException e) {
                e.printStackTrace();
                Assert.assertEquals(e.getCode(), 115);
                Assert.assertTrue(e.getMessage().startsWith("Code: 115. DB::Exception: Setting unknown_setting is neither a builtin setting nor started with the prefix 'custom_' registered for user-defined settings. (UNKNOWN_SETTING)"),
                        "Unexpected error message: " + e.getMessage());
            }

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage(), e);
        }

        try (Client client = new Client.Builder()
                .addEndpoint(server.getBaseUri())
                .setUsername("non-existing-user")
                .setPassword("nothing")
                .compressServerResponse(serverCompression)
                .useHttpCompression(useHttpCompression)
                .build()) {

            try (QueryResponse response = client.query("SELECT 1").get(1, TimeUnit.SECONDS)) {
                Assert.fail("Expected exception");
            } catch (ServerException e) {
                e.printStackTrace();
                Assert.assertEquals(e.getCode(), 516);
                Assert.assertTrue(e.getMessage().startsWith("Code: 516. DB::Exception: non-existing-user: Authentication failed: password is incorrect, or there is no user with such name. (AUTHENTICATION_FAILED)"),
                        e.getMessage());
            } catch (Exception e) {
                e.printStackTrace();
                Assert.fail("Unexpected exception", e);
            }
        }
    }

    @DataProvider(name = "testServerErrorHandlingDataProvider")
    public static Object[][] testServerErrorHandlingDataProvider() {
        EnumSet<ClickHouseFormat> formats = EnumSet.of(ClickHouseFormat.CSV, ClickHouseFormat.TSV,
                                            ClickHouseFormat.JSON, ClickHouseFormat.JSONCompact);

        Object[][] result = new Object[formats.size() * 3][];

        int i = 0;
        for (ClickHouseFormat format : formats) {
            result[i++] = new Object[]{format, false, false};
            result[i++] = new Object[]{format, true, false};
            result[i++] = new Object[]{format, true, true};
        }

        return result;
    }

    @Test(groups = { "integration" })
    public void testErrorWithSuccessfulResponse() {
        WireMockServer mockServer = new WireMockServer( WireMockConfiguration
                .options().port(9090).notifier(new ConsoleNotifier(false)));
        mockServer.start();

        try (Client client = new Client.Builder().addEndpoint(Protocol.HTTP, "localhost", mockServer.port(), false)
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .compressServerResponse(false)
                .useNewImplementation(true)
                .build()) {
            mockServer.addStubMapping(WireMock.post(WireMock.anyUrl())
                    .willReturn(WireMock.aResponse()
                            .withStatus(HttpStatus.SC_OK)
                            .withChunkedDribbleDelay(2, 200)
                            .withHeader("X-ClickHouse-Exception-Code", "241")
                            .withHeader("X-ClickHouse-Summary",
                                    "{ \"read_bytes\": \"10\", \"read_rows\": \"1\"}")
                            .withBody("Code: 241. DB::Exception: Memory limit (for query) exceeded: would use 97.21 MiB"))
                    .build());

            try (QueryResponse response = client.query("SELECT 1").get(1, TimeUnit.SECONDS)) {
                Assert.fail("Expected exception");
            } catch (ServerException e) {
                e.printStackTrace();
                Assert.assertEquals(e.getMessage(), "Code: 241. DB::Exception: Memory limit (for query) exceeded: would use 97.21 MiB");
            } catch (Exception e) {
                e.printStackTrace();
                Assert.fail("Unexpected exception", e);
            }
        } finally {
            mockServer.stop();
        }
    }

    @Test(groups = { "integration" }, dataProvider = "testServerErrorsUncompressedDataProvider")
    public void testServerErrorsUncompressed(int code, String message, String expectedMessage) {
        if (isCloud()) {
            return; // mocked server
        }

        WireMockServer mockServer = new WireMockServer( WireMockConfiguration
                .options().port(9090).notifier(new ConsoleNotifier(false)));
        mockServer.start();

        mockServer.addStubMapping(WireMock.post(WireMock.anyUrl())
                .willReturn(WireMock.aResponse()
                        .withStatus(HttpStatus.SC_OK)
                        .withChunkedDribbleDelay(2, 200)
                        .withHeader("X-ClickHouse-Exception-Code", String.valueOf(code))
                        .withHeader("X-ClickHouse-Summary",
                                "{ \"read_bytes\": \"10\", \"read_rows\": \"1\"}")
                        .withBody(message))
                .build());

        try (Client client = new Client.Builder().addEndpoint(Protocol.HTTP, "localhost", mockServer.port(), false)
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .compressServerResponse(false)
                .build()) {

            try (QueryResponse response = client.query("SELECT 1").get(1, TimeUnit.SECONDS)) {
                Assert.fail("Expected exception");
            } catch (ServerException e) {
                e.printStackTrace();
                Assert.assertEquals(e.getCode(), code);
                Assert.assertEquals(e.getMessage(), expectedMessage);
            } catch (Exception e) {
                e.printStackTrace();
                Assert.fail("Unexpected exception", e);
            }
        } finally {
            mockServer.stop();
        }
    }

    @DataProvider(name = "testServerErrorsUncompressedDataProvider")
    public static Object[][] testServerErrorsUncompressedDataProvider() {
        return new Object[][] {
                { 241, "Code: 241. DB::Exception: Memory limit (for query) exceeded: would use 97.21 MiB",
                        "Code: 241. DB::Exception: Memory limit (for query) exceeded: would use 97.21 MiB"},
                {900, "Code: 900. DB::Exception: \uD83D\uDCBE Floppy disk is full",
                        "Code: 900. DB::Exception: \uD83D\uDCBE Floppy disk is full"},
                {901, "Code: 901. DB::Exception: I write, erase, rewrite\n" +
                        "Erase again, and then\n" +
                        "A poppy blooms\n" +
                        " (by Katsushika Hokusai)",
                        "Code: 901. DB::Exception: I write, erase, rewrite " +
                                "Erase again, and then " +
                                "A poppy blooms" +
                                " (by Katsushika Hokusai)"}
        };
    }

    @Test(groups = { "integration" })
    public void testAdditionalHeaders() {
        if (isCloud()) {
            return; // mocked server
        }

        WireMockServer mockServer = new WireMockServer( WireMockConfiguration
                .options().port(9090).notifier(new ConsoleNotifier(false)));
        mockServer.start();


        try (Client client = new Client.Builder().addEndpoint(Protocol.HTTP, "localhost", mockServer.port(), false)
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .useNewImplementation(true)
                .httpHeader("X-ClickHouse-Test", "default_value")
                .httpHeader("X-ClickHouse-Test-2", Arrays.asList("default_value1", "default_value2"))
                .httpHeader("X-ClickHouse-Test-3", Arrays.asList("default_value1", "default_value2"))
                .httpHeader("X-ClickHouse-Test-4", "default_value4")
                .build()) {
            mockServer.addStubMapping(WireMock.post(WireMock.anyUrl())
                    .withHeader("X-ClickHouse-Test", WireMock.equalTo("test"))
                    .withHeader("X-ClickHouse-Test-2", WireMock.equalTo("test1,test2"))
                    .withHeader("X-ClickHouse-Test-3", WireMock.equalTo("default_value1,default_value2"))
                    .withHeader("X-ClickHouse-Test-4", WireMock.equalTo("default_value4"))

                    .willReturn(WireMock.aResponse()
                            .withHeader("X-ClickHouse-Summary",
                                    "{ \"read_bytes\": \"10\", \"read_rows\": \"1\"}")).build());

            QuerySettings querySettings = new QuerySettings()
                    .httpHeader("X-ClickHouse-Test", "test")
                    .httpHeader("X-ClickHouse-Test-2", Arrays.asList("test1", "test2"));

            try (QueryResponse response = client.query("SELECT 1", querySettings).get(10, TimeUnit.SECONDS)) {
                Assert.assertEquals(response.getReadBytes(), 10);
            } catch (Exception e) {
                e.printStackTrace();
                Assert.fail("Unexpected exception", e);
            }
        } finally {
            mockServer.stop();
        }
    }

    @Test(groups = { "integration" })
    public void testServerSettings() {
        if (isCloud()) {
            return; // mocked server
        }

        WireMockServer mockServer = new WireMockServer( WireMockConfiguration
                .options().port(9090).notifier(new ConsoleNotifier(false)));
        mockServer.start();

        try (Client client = new Client.Builder().addEndpoint(Protocol.HTTP, "localhost", mockServer.port(), false)
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .useNewImplementation(true)
                .serverSetting("max_threads", "10")
                .serverSetting("async_insert", "1")
                .serverSetting("roles", Arrays.asList("role1", "role2"))
                .compressClientRequest(true)
                .build()) {

            mockServer.addStubMapping(WireMock.post(WireMock.anyUrl())
                            .withQueryParam("max_threads", WireMock.equalTo("10"))
                            .withQueryParam("async_insert", WireMock.equalTo("1"))
                            .withQueryParam("roles", WireMock.equalTo("role3,role2"))
                            .withQueryParam("compress", WireMock.equalTo("0"))
                    .willReturn(WireMock.aResponse()
                            .withHeader("X-ClickHouse-Summary",
                                    "{ \"read_bytes\": \"10\", \"read_rows\": \"1\"}")).build());

            QuerySettings querySettings = new QuerySettings()
                    .serverSetting("max_threads", "10")
                    .serverSetting("async_insert", "3")
                    .serverSetting("roles", Arrays.asList("role3", "role2"))
                    .serverSetting("compress", "0");
            try (QueryResponse response = client.query("SELECT 1", querySettings).get(1, TimeUnit.SECONDS)) {
                Assert.assertEquals(response.getReadBytes(), 10);
            } catch (Exception e) {
                e.printStackTrace();
                Assert.fail("Unexpected exception", e);
            } finally {
                mockServer.stop();
            }
        }
    }

    static {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "DEBUG");
    }

    @Test(groups = { "integration" })
    public void testSSLAuthentication() throws Exception {
        if (isCloud()) {
            return; // Current test is working only with local server because of self-signed certificates.
        }
        ClickHouseNode server = getSecureServer(ClickHouseProtocol.HTTP);
        try (Client client = new Client.Builder().addEndpoint(Protocol.HTTP, "localhost",server.getPort(), true)
                .setUsername("dba")
                .setPassword("dba")
                .setRootCertificate("containers/clickhouse-server/certs/localhost.crt")
                .build()) {

            try (CommandResponse resp = client.execute("DROP USER IF EXISTS some_user").get()) {
            }
            try (CommandResponse resp = client.execute("CREATE USER some_user IDENTIFIED WITH ssl_certificate CN 'some_user'").get()) {
            }
        }

        try (Client client = new Client.Builder().addEndpoint(Protocol.HTTP, "localhost",server.getPort(), true)
                .useSSLAuthentication(true)
                .setUsername("some_user")
                .setRootCertificate("containers/clickhouse-server/certs/localhost.crt")
                .setClientCertificate("some_user.crt")
                .setClientKey("some_user.key")
                .compressServerResponse(false)
                .build()) {

            try (QueryResponse resp = client.query("SELECT 1").get()) {
                Assert.assertEquals(resp.getReadRows(), 1);
            }
        }
    }

    @Test(groups = { "integration" }, dataProvider = "testPasswordAuthenticationProvider", dataProviderClass = HttpTransportTests.class)
    public void testPasswordAuthentication(String identifyWith, String identifyBy, boolean failsWithHeaders) throws Exception {
        if (isCloud()) {
            return; // Current test is working only with local server because of self-signed certificates.
        }
        ClickHouseNode server = getServer(ClickHouseProtocol.HTTP);

        try (Client client = new Client.Builder().addEndpoint(Protocol.HTTP, "localhost",server.getPort(), false)
                .setUsername("dba")
                .setPassword("dba")
                .build()) {

            try (CommandResponse resp = client.execute("DROP USER IF EXISTS some_user").get()) {
            }
            try (CommandResponse resp = client.execute("CREATE USER some_user IDENTIFIED WITH " + identifyWith + " BY '" + identifyBy + "'").get()) {
            }
        } catch (Exception e) {
            Assert.fail("Failed on setup", e);
        }


        try (Client client = new Client.Builder().addEndpoint(Protocol.HTTP, "localhost",server.getPort(), false)
                .setUsername("some_user")
                .setPassword(identifyBy)
                .build()) {

            Assert.assertEquals(client.queryAll("SELECT user()").get(0).getString(1), "some_user");
        } catch (Exception e) {
            Assert.fail("Failed to authenticate", e);
        }

        if (failsWithHeaders) {
            try (Client client = new Client.Builder().addEndpoint(Protocol.HTTP, "localhost",server.getPort(), false)
                    .setUsername("some_user")
                    .setPassword(identifyBy)
                    .useHTTPBasicAuth(false)
                    .build()) {

                Assert.expectThrows(ClientException.class, () ->
                        client.queryAll("SELECT user()").get(0).getString(1));

            } catch (Exception e) {
                Assert.fail("Unexpected exception", e);
            }
        }
    }

    @DataProvider(name = "testPasswordAuthenticationProvider")
    public static Object[][] testPasswordAuthenticationProvider() {
        return new Object[][] {
                { "plaintext_password", "password", false},
                { "plaintext_password", "", false },
                { "plaintext_password", "S3Cr=?t", true},
                { "plaintext_password", "123§", true },
                { "sha256_password", "password", false },
                { "sha256_password", "123§", true },
                { "sha256_password", "S3Cr=?t", true},
                { "sha256_password", "S3Cr?=t", false},
        };
    }

    @Test(groups = { "integration" })
    public void testAuthHeaderIsKeptFromUser() throws Exception {
        if (isCloud()) {
            return; // Current test is working only with local server because of self-signed certificates.
        }
        ClickHouseNode server = getServer(ClickHouseProtocol.HTTP);

        String identifyWith = "sha256_password";
        String identifyBy = "123§";
        try (Client client = new Client.Builder().addEndpoint(Protocol.HTTP, "localhost",server.getPort(), false)
                .setUsername("dba")
                .setPassword("dba")
                .build()) {

            try (CommandResponse resp = client.execute("DROP USER IF EXISTS some_user").get()) {
            }
            try (CommandResponse resp = client.execute("CREATE USER some_user IDENTIFIED WITH " + identifyWith + " BY '" + identifyBy + "'").get()) {
            }
        } catch (Exception e) {
            Assert.fail("Failed on setup", e);
        }


        try (Client client = new Client.Builder().addEndpoint(Protocol.HTTP, "localhost",server.getPort(), false)
                .setUsername("some_user")
                .setPassword(identifyBy)
                .useHTTPBasicAuth(false) // disable basic auth to produce CH headers
                .httpHeader(HttpHeaders.AUTHORIZATION, "Basic " + Base64.getEncoder().encodeToString(("some_user:" +identifyBy).getBytes()))
                .build()) {

            Assert.assertEquals(client.queryAll("SELECT user()").get(0).getString(1), "some_user");
        } catch (Exception e) {
            Assert.fail("Failed to authenticate", e);
        }
    }

    @Test(groups = { "integration" })
    public void testSSLAuthentication_invalidConfig() throws Exception {
        if (isCloud()) {
            return; // Current test is working only with local server because of self-signed certificates.
        }
        ClickHouseNode server = getSecureServer(ClickHouseProtocol.HTTP);
        try (Client client = new Client.Builder().addEndpoint(Protocol.HTTP, "localhost",server.getPort(), true)
                .useSSLAuthentication(true)
                .setUsername("some_user")
                .setPassword("s3cret")
                .setRootCertificate("containers/clickhouse-server/certs/localhost.crt")
                .setClientCertificate("some_user.crt")
                .setClientKey("some_user.key")
                .compressServerResponse(false)
                .build()) {
            fail("Expected exception");
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
                Assert.assertTrue(e.getMessage().startsWith("Only one of password, access token or SSL authentication"));
        }
    }

    @Test(groups = { "integration" })
    public void testErrorWithSendProgressHeaders() throws Exception {
        if (isCloud()) {
            return; // mocked server
        }

        ClickHouseNode server = getServer(ClickHouseProtocol.HTTP);
        try (Client client = new Client.Builder().addEndpoint(Protocol.HTTP, "localhost",server.getPort(), false)
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .useNewImplementation(true)
                .build()) {

            try (CommandResponse resp = client.execute("DROP TABLE IF EXISTS test_omm_table").get()) {
            }
            try (CommandResponse resp = client.execute("CREATE TABLE test_omm_table ( val String) Engine = MergeTree ORDER BY () ").get()) {
            }

            QuerySettings settings = new QuerySettings()
                    .serverSetting("send_progress_in_http_headers", "1")
                    .serverSetting("max_memory_usage", "54M");

            try (QueryResponse resp = client.query("INSERT INTO test_omm_table SELECT randomString(16) FROM numbers(300000000)", settings).get()) {

            } catch (ServerException e) {
                Assert.assertEquals(e.getCode(), 241);
            }
        }
    }


    @Test(groups = { "integration" }, dataProvider = "testUserAgentHasCompleteProductName_dataProvider", dataProviderClass = HttpTransportTests.class)
    public void testUserAgentHasCompleteProductName(String clientName, Pattern userAgentPattern) throws Exception {
        if (isCloud()) {
            return; // mocked server
        }

        ClickHouseNode server = getServer(ClickHouseProtocol.HTTP);
        try (Client client = new Client.Builder()
                .addEndpoint(server.getBaseUri())
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .setClientName(clientName)
                .build()) {

            String q1Id = UUID.randomUUID().toString();

            client.execute("SELECT 1", (CommandSettings) new CommandSettings().setQueryId(q1Id)).get().close();
            client.execute("SYSTEM FLUSH LOGS").get().close();

            List<GenericRecord> logRecords = client.queryAll("SELECT http_user_agent, http_referer, " +
                    " forwarded_for  FROM system.query_log WHERE query_id = '" + q1Id + "'");
            Assert.assertFalse(logRecords.isEmpty(), "No records found in query log");

            for (GenericRecord record : logRecords) {
                System.out.println(record.getString("http_user_agent"));
                Assert.assertTrue(userAgentPattern.matcher(record.getString("http_user_agent")).matches(),
                        record.getString("http_user_agent") + " doesn't match \"" +
                                  userAgentPattern.pattern() + "\"");

            }
        }
    }


    @DataProvider(name = "testUserAgentHasCompleteProductName_dataProvider")
    public static Object[][] testUserAgentHasCompleteProductName_dataProvider() {
        return new Object[][] {
                { "", Pattern.compile("clickhouse-java-v2\\/.+ \\(.+\\) Apache-HttpClient\\/[\\d\\.]+$") },
                { "test-client/1.0", Pattern.compile("test-client/1.0 clickhouse-java-v2\\/.+ \\(.+\\) Apache-HttpClient\\/[\\d\\.]+$")},
                { "test-client/", Pattern.compile("test-client/ clickhouse-java-v2\\/.+ \\(.+\\) Apache-HttpClient\\/[\\d\\.]+$")}};
    }

    @Test(dataProvider = "testClientNameDataProvider")
    public void testClientName(String clientName, boolean setWithUpdate, String userAgentHeader, boolean setForRequest) throws Exception {

        final String initialClientName = setWithUpdate ? "init clientName" : clientName;
        final String initialUserAgentHeader = setForRequest ? "init userAgentHeader" : userAgentHeader;
        final String clientReferer = "http://localhost/webpage";

        Client.Builder builder = newClient();
        if (initialClientName != null) {
            builder.setClientName(initialClientName);
        }
        if (initialUserAgentHeader != null) {
            builder.httpHeader(HttpHeaders.USER_AGENT, initialUserAgentHeader);
        }
        try (Client client = builder.build()) {
            String expectedClientNameStartsWith = initialClientName == null || initialUserAgentHeader != null ? initialUserAgentHeader : initialClientName;

            if (setWithUpdate) {
                client.updateClientName(clientName);
                expectedClientNameStartsWith = initialUserAgentHeader == null ? clientName : initialUserAgentHeader;
            }

            String qId = UUID.randomUUID().toString();
            QuerySettings settings = new QuerySettings()
                    .httpHeader(HttpHeaders.REFERER, clientReferer)
                    .setQueryId(qId);

            if (setForRequest) {
                settings.httpHeader(HttpHeaders.USER_AGENT, userAgentHeader);
                expectedClientNameStartsWith = userAgentHeader;
            }

            client.query("SELECT 1", settings).get().close();
            client.execute("SYSTEM FLUSH LOGS").get().close();

            List<GenericRecord> logRecords = client.queryAll("SELECT query_id, client_name, http_user_agent, http_referer " +
                    " FROM system.query_log WHERE query_id = '" + settings.getQueryId() + "'");
            Assert.assertEquals(logRecords.get(0).getString("query_id"), settings.getQueryId());
            final String logUserAgent = logRecords.get(0).getString("http_user_agent");
            Assert.assertTrue(logUserAgent.startsWith(expectedClientNameStartsWith),
                    "Expected to start with \"" + expectedClientNameStartsWith + "\" but values was \"" + logUserAgent + "\"" );
            Assert.assertTrue(logUserAgent.contains(Client.CLIENT_USER_AGENT), "Expected to contain client v2 version but value was \"" + logUserAgent + "\"");
            Assert.assertEquals(logRecords.get(0).getString("http_referer"), clientReferer);
            Assert.assertEquals(logRecords.get(0).getString("client_name"), ""); // http client can't set this field
        }
    }

    @DataProvider(name = "testClientNameDataProvider")
    public static Object[][] testClientName() {
        return new Object[][] {
                {"test-product (app 1.0)", false, null, false}, // only client name set
                {"test-product (app 1.0)", false, "final product (app 1.1)", false}, // http header set and overrides client name
                {"test-product (app 1.0)", true, null, false}, // client name set thru Client#updateClientName
                {"test-product (app 1.0)", true, "final product (app 1.1)", true}, // custom UserAgent header overrides client name
        };
    }

    @Test(dataProvider = "testClientNameThruRawOptionsDataProvider")
    public void testClientNameThruRawOptions(String property, String value, boolean setInClient) throws Exception {
        Client.Builder builder = newClient();
        if (setInClient) {
            builder.setOption(property, value);
        }
        try (Client client = builder.build()) {

            String qId = UUID.randomUUID().toString();
            QuerySettings settings = new QuerySettings()
                    .setQueryId(qId);

            if (!setInClient) {
                settings.setOption(property, value);
            }

            client.query("SELECT 1", settings).get().close();
            client.execute("SYSTEM FLUSH LOGS").get().close();

            List<GenericRecord> logRecords = client.queryAll("SELECT query_id, client_name, http_user_agent, http_referer " +
                    " FROM system.query_log WHERE query_id = '" + settings.getQueryId() + "'");
            Assert.assertEquals(logRecords.get(0).getString("query_id"), settings.getQueryId());
            final String logUserAgent = logRecords.get(0).getString("http_user_agent");
            Assert.assertTrue(logUserAgent.startsWith(value),
                    "Expected to start with \"" + value + "\" but values was \"" + logUserAgent + "\"" );
            Assert.assertTrue(logUserAgent.contains(Client.CLIENT_USER_AGENT), "Expected to contain client v2 version but value was \"" + logUserAgent + "\"");
        }
    }

    @DataProvider(name = "testClientNameThruRawOptionsDataProvider")
    public Object[][] testClientNameThruRawOptionsDataProvider() {
        return new Object[][] {
                {ClientConfigProperties.PRODUCT_NAME.getKey(), "my product 1", true},
                {ClientConfigProperties.CLIENT_NAME.getKey(), "my product 2", true},
                {ClientConfigProperties.PRODUCT_NAME.getKey(), "my product 1", false},
                {ClientConfigProperties.CLIENT_NAME.getKey(), "my product 2", false},
        };
    }

    @Test(groups = { "integration" })
    public void testBearerTokenAuth() throws Exception {
        if (isCloud()) {
            return; // mocked server
        }

        WireMockServer mockServer = new WireMockServer( WireMockConfiguration
                .options().port(9090).notifier(new ConsoleNotifier(false)));
        mockServer.start();

        try {
            String jwtToken1 = Arrays.stream(
                            new String[]{"header", "payload", "signature"})
                    .map(s -> Base64.getEncoder().encodeToString(s.getBytes(StandardCharsets.UTF_8)))
                    .reduce((s1, s2) -> s1 + "." + s2).get();
            try (Client client = new Client.Builder().addEndpoint(Protocol.HTTP, "localhost", mockServer.port(), false)
                    .useBearerTokenAuth(jwtToken1)
                    .compressServerResponse(false)
                    .build()) {

                mockServer.addStubMapping(WireMock.post(WireMock.anyUrl())
                        .withHeader("Authorization", WireMock.equalTo("Bearer " + jwtToken1))
                        .willReturn(WireMock.aResponse()
                                .withHeader("X-ClickHouse-Summary",
                                        "{ \"read_bytes\": \"10\", \"read_rows\": \"1\"}")).build());

                try (QueryResponse response = client.query("SELECT 1").get(1, TimeUnit.SECONDS)) {
                    Assert.assertEquals(response.getReadBytes(), 10);
                } catch (Exception e) {
                    Assert.fail("Unexpected exception", e);
                }
            }

            String jwtToken2 = Arrays.stream(
                            new String[]{"header2", "payload2", "signature2"})
                    .map(s -> Base64.getEncoder().encodeToString(s.getBytes(StandardCharsets.UTF_8)))
                    .reduce((s1, s2) -> s1 + "." + s2).get();

            mockServer.resetAll();
            mockServer.addStubMapping(WireMock.post(WireMock.anyUrl())
                    .withHeader("Authorization", WireMock.equalTo("Bearer " + jwtToken1))
                    .willReturn(WireMock.aResponse()
                            .withStatus(HttpStatus.SC_UNAUTHORIZED))
                    .build());

            try (Client client = new Client.Builder().addEndpoint(Protocol.HTTP, "localhost", mockServer.port(), false)
                    .useBearerTokenAuth(jwtToken1)
                    .compressServerResponse(false)
                    .build()) {

                try {
                    client.execute("SELECT 1").get();
                    fail("Exception expected");
                } catch (ServerException e) {
                    Assert.assertEquals(e.getTransportProtocolCode(), HttpStatus.SC_UNAUTHORIZED);
                }

                mockServer.resetAll();
                mockServer.addStubMapping(WireMock.post(WireMock.anyUrl())
                        .withHeader("Authorization", WireMock.equalTo("Bearer " + jwtToken2))
                        .willReturn(WireMock.aResponse()
                                .withHeader("X-ClickHouse-Summary",
                                        "{ \"read_bytes\": \"10\", \"read_rows\": \"1\"}"))

                        .build());

                client.updateBearerToken(jwtToken2);

                client.execute("SELECT 1").get();
            }
        } finally {
            mockServer.stop();
        }
    }

    @Test(groups = { "integration" })
    public void testJWTWithCloud() throws Exception {
        if (!isCloud()) {
            return; // only for cloud
        }
        String jwt = System.getenv("CLIENT_JWT");
        Assert.assertTrue(jwt != null && !jwt.trim().isEmpty(), "JWT is missing");
        Assert.assertFalse(jwt.contains("\n") || jwt.contains("-----"), "JWT should be single string ready for HTTP header");
        try (Client client = newClient().useBearerTokenAuth(jwt).build()) {
            try {
                List<GenericRecord> response = client.queryAll("SELECT user(), now()");
                System.out.println("response: " + response.get(0).getString(1) + " time: " + response.get(0).getString(2));
            } catch (Exception e) {
                e.printStackTrace();
                throw e;
            }
        }
    }

    @Test(groups = { "integration" })
    public void testWithDefaultTimeouts() {
        if (isCloud()) {
            return; // mocked server
        }

        int proxyPort = new Random().nextInt(1000) + 10000;
        WireMockServer proxy = new WireMockServer(WireMockConfiguration
                .options().port(proxyPort)
                .notifier(new Slf4jNotifier(true)));
        proxy.start();
        proxy.addStubMapping(WireMock.post(WireMock.anyUrl())
                .willReturn(WireMock.aResponse().withFixedDelay(5000)
                        .withStatus(HttpStatus.SC_OK)
                        .withHeader("X-ClickHouse-Summary", "{ \"read_bytes\": \"10\", \"read_rows\": \"1\"}")).build());

        try (Client client = new Client.Builder().addEndpoint(Protocol.HTTP, "localhost", proxyPort, false)
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .useNewImplementation(true)
                .build()) {
            int startTime = (int) System.currentTimeMillis();
            try {
                client.query("SELECT 1").get();
            } catch (Exception e) {
                Assert.fail("Elapsed Time: " + (System.currentTimeMillis() - startTime), e);
            }
        } finally {
            proxy.stop();
        }
    }


    @Test(groups = { "integration" })
    public void testTimeoutsWithRetry() {
        if (isCloud()) {
            return; // mocked server
        }

        WireMockServer faultyServer = new WireMockServer( WireMockConfiguration
                .options().port(9090).notifier(new ConsoleNotifier(false)));
        faultyServer.start();

        // First request gets no response
        faultyServer.addStubMapping(WireMock.post(WireMock.anyUrl())
                .inScenario("Timeout")
                .withRequestBody(WireMock.containing("SELECT 1"))
                .whenScenarioStateIs(STARTED)
                .willSetStateTo("Failed")
                .willReturn(WireMock.aResponse()
                        .withStatus(HttpStatus.SC_OK)
                        .withFixedDelay(5000)
                        .withHeader("X-ClickHouse-Summary",
                        "{ \"read_bytes\": \"10\", \"read_rows\": \"1\"}")).build());

        // Second request gets a response (retry)
        faultyServer.addStubMapping(WireMock.post(WireMock.anyUrl())
                .inScenario("Timeout")
                .withRequestBody(WireMock.containing("SELECT 1"))
                .whenScenarioStateIs("Failed")
                .willSetStateTo("Done")
                .willReturn(WireMock.aResponse()
                        .withStatus(HttpStatus.SC_OK)
                        .withFixedDelay(1000)
                        .withHeader("X-ClickHouse-Summary",
                                "{ \"read_bytes\": \"10\", \"read_rows\": \"1\"}")).build());

        try (Client client = new Client.Builder().addEndpoint(Protocol.HTTP, "localhost", faultyServer.port(), false)
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .setSocketTimeout(3000)
                .retryOnFailures(ClientFaultCause.SocketTimeout)
                .build()) {
            int startTime = (int) System.currentTimeMillis();
            try {
                client.query("SELECT 1").get();
            } catch (Exception e) {
                Assert.fail("Elapsed Time: " + (System.currentTimeMillis() - startTime), e);
            }
        } finally {
            faultyServer.stop();
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

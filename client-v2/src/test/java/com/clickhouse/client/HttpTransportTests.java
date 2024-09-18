package com.clickhouse.client;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.ClientFaultCause;
import com.clickhouse.client.api.ConnectionInitiationException;
import com.clickhouse.client.api.ConnectionReuseStrategy;
import com.clickhouse.client.api.ServerException;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.enums.ProxyType;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.data.ClickHouseFormat;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.ConsoleNotifier;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.http.Fault;
import com.github.tomakehurst.wiremock.http.trafficlistener.WiremockNetworkTrafficListener;
import org.apache.hc.core5.http.ConnectionRequestTimeoutException;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.net.URIBuilder;
import org.testcontainers.utility.ThrowingFunction;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;

public class HttpTransportTests extends BaseIntegrationTest{

    static {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "DEBUG");
    }

    @Test(groups = {"integration"},dataProvider = "testConnectionTTLProvider")
    @SuppressWarnings("java:S2925")
    public void testConnectionTTL(Long connectionTtl, Long keepAlive, int openSockets) throws Exception {
        if (isCloud()) {
            return; // skip cloud tests because of wiremock proxy. TODO: fix it
        }
        ClickHouseNode server = getServer(ClickHouseProtocol.HTTP);

        int proxyPort = new Random().nextInt(1000) + 10000;
        System.out.println("proxyPort: " + proxyPort);
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

        int serverPort = new Random().nextInt(1000) + 10000;
        System.out.println("proxyPort: " + serverPort);
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

    @Test(groups = { "integration" }, dataProvider = "NoResponseFailureProvider")
    public void testInsertAndNoHttpResponseFailure(String body, int maxRetries, ThrowingFunction<Client, Void> function,
                                                   boolean shouldFail) {
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
                .setPassword("")
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

        String insertBody = "INSERT INTO table01 FORMAT " + ClickHouseFormat.TSV.name() + " \n1\t2\t3\n";
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
    public void testServerErrorHandling(ClickHouseFormat format) {
        ClickHouseNode server = getServer(ClickHouseProtocol.HTTP);
        try (Client client = new Client.Builder()
                .addEndpoint(server.getBaseUri())
                .setUsername("default")
                .setPassword("")
                .useNewImplementation(true)
                // TODO: fix in old client
//                .useNewImplementation(System.getProperty("client.tests.useNewImplementation", "false").equals("true"))
                .build()) {

            QuerySettings querySettings = new QuerySettings().setFormat(format);
            try (QueryResponse response =
                         client.query("SELECT invalid;statement", querySettings).get(1, TimeUnit.SECONDS)) {
                Assert.fail("Expected exception");
            } catch (ClientException e) {
                e.printStackTrace();
                ServerException serverException = (ServerException) e.getCause();
                Assert.assertEquals(serverException.getCode(), 62);
                Assert.assertTrue(serverException.getMessage().startsWith("Code: 62. DB::Exception: Syntax error (Multi-statements are not allowed): failed at position 15 (end of query)"),
                        "Unexpected error message: " + serverException.getMessage());
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage(), e);
        }
    }

    @DataProvider(name = "testServerErrorHandlingDataProvider")
    public static Object[] testServerErrorHandlingDataProvider() {
        return new Object[] { ClickHouseFormat.JSON, ClickHouseFormat.TabSeparated, ClickHouseFormat.RowBinary };
    }
}

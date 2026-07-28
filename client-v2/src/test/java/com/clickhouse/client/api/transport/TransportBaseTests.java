package com.clickhouse.client.api.transport;


import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientFaultCause;
import com.clickhouse.client.api.ServerException;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.ConsoleNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseFormat;
import org.apache.hc.core5.http.HttpStatus;
import org.testcontainers.utility.ThrowingFunction;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;

@Test(groups = {"integration"})
public class TransportBaseTests extends BaseIntegrationTest {

    private static final String RETRYABLE_BODY =
            "Code: 202. DB::Exception: Too many simultaneous queries. (TOO_MANY_SIMULTANEOUS_QUERIES)";
    private static final int RETRYABLE_CODE = 202;

    private static final String NON_RETRYABLE_BODY =
            "Code: 62. DB::Exception: Syntax error. (SYNTAX_ERROR)";
    private static final int NON_RETRYABLE_CODE = 62;

    private WireMockServer startMockServer() {
        WireMockServer mockServer = new WireMockServer(WireMockConfiguration
                .options().dynamicPort().notifier(new ConsoleNotifier(false)));
        mockServer.start();
        return mockServer;
    }

    private Client mockServerClient(WireMockServer mockServer, int maxRetries, ClientFaultCause... retryOn) {
        Client.Builder builder = new Client.Builder()
                .addEndpoint(Protocol.HTTP, "localhost", mockServer.port(), false)
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .compressClientRequest(false)
                .compressServerResponse(false)
                .setMaxRetries(maxRetries);
        if (retryOn.length > 0) {
            builder.retryOnFailures(retryOn);
        }
        return builder.build();
    }

    /**
     * A retryable server error on the first attempt should be retried and the following successful
     * response should be returned. Covers the retry branch of the operation loop
     * ({@code shouldRetry(...)} == true) and recovery on the next node.
     */
    @Test(groups = {"integration"}, dataProvider = "operationProvider")
    public void testRetriesAndSucceedsAfterRetryableServerError(String operation,
                                                                ThrowingFunction<Client, Void> function) {
        if (isCloud()) {
            return; // mocked server
        }

        WireMockServer mockServer = startMockServer();

        mockServer.addStubMapping(WireMock.post(WireMock.anyUrl())
                .inScenario("Retry")
                .whenScenarioStateIs(STARTED)
                .willSetStateTo("Recovered")
                .willReturn(WireMock.aResponse()
                        .withStatus(HttpStatus.SC_SERVICE_UNAVAILABLE)
                        .withHeader("X-ClickHouse-Exception-Code", String.valueOf(RETRYABLE_CODE))
                        .withBody(RETRYABLE_BODY)).build());

        mockServer.addStubMapping(WireMock.post(WireMock.anyUrl())
                .inScenario("Retry")
                .whenScenarioStateIs("Recovered")
                .willReturn(WireMock.aResponse()
                        .withStatus(HttpStatus.SC_OK)
                        .withHeader("X-ClickHouse-Summary",
                                "{ \"read_bytes\": \"10\", \"read_rows\": \"1\"}")).build());

        try (Client client = mockServerClient(mockServer, 1)) {
            function.apply(client);
        } catch (Exception e) {
            Assert.fail("[" + operation + "] should have recovered after a retry", e);
        } finally {
            mockServer.stop();
        }

        Assert.assertEquals(mockServer.findAll(WireMock.postRequestedFor(WireMock.anyUrl())).size(), 2,
                "[" + operation + "] expected one failed attempt followed by a successful retry");
    }

    /**
     * When the server keeps returning a retryable error the operation should be retried
     * {@code maxRetries} times and then fail by re-throwing the last captured exception.
     * Covers the final throw of the operation loop ({@code throw lastException}).
     */
    @Test(groups = {"integration"}, dataProvider = "operationProvider")
    public void testThrowsAfterExhaustingRetries(String operation,
                                                 ThrowingFunction<Client, Void> function) {
        if (isCloud()) {
            return; // mocked server
        }

        int maxRetries = 2;
        WireMockServer mockServer = startMockServer();

        mockServer.addStubMapping(WireMock.post(WireMock.anyUrl())
                .willReturn(WireMock.aResponse()
                        .withStatus(HttpStatus.SC_SERVICE_UNAVAILABLE)
                        .withHeader("X-ClickHouse-Exception-Code", String.valueOf(RETRYABLE_CODE))
                        .withBody(RETRYABLE_BODY)).build());

        try (Client client = mockServerClient(mockServer, maxRetries)) {
            function.apply(client);
            Assert.fail("[" + operation + "] expected exception after exhausting retries");
        } catch (ServerException e) {
            Assert.assertEquals(e.getCode(), RETRYABLE_CODE);
        } catch (Exception e) {
            Assert.fail("[" + operation + "] unexpected exception type", e);
        } finally {
            mockServer.stop();
        }

        Assert.assertEquals(mockServer.findAll(WireMock.postRequestedFor(WireMock.anyUrl())).size(), maxRetries + 1,
                "[" + operation + "] expected initial attempt plus " + maxRetries + " retries");
    }

    /**
     * A non-retryable server error should be re-thrown immediately without consuming any of the
     * configured retries. Covers the {@code else { throw lastException; }} branch.
     */
    @Test(groups = {"integration"}, dataProvider = "operationProvider")
    public void testThrowsImmediatelyWhenNotRetryable(String operation,
                                                      ThrowingFunction<Client, Void> function) {
        if (isCloud()) {
            return; // mocked server
        }

        WireMockServer mockServer = startMockServer();

        mockServer.addStubMapping(WireMock.post(WireMock.anyUrl())
                .willReturn(WireMock.aResponse()
                        .withStatus(HttpStatus.SC_BAD_REQUEST)
                        .withHeader("X-ClickHouse-Exception-Code", String.valueOf(NON_RETRYABLE_CODE))
                        .withBody(NON_RETRYABLE_BODY)).build());

        try (Client client = mockServerClient(mockServer, 3)) {
            function.apply(client);
            Assert.fail("[" + operation + "] expected exception for non-retryable error");
        } catch (ServerException e) {
            Assert.assertEquals(e.getCode(), NON_RETRYABLE_CODE);
        } catch (Exception e) {
            Assert.fail("[" + operation + "] unexpected exception type", e);
        } finally {
            mockServer.stop();
        }

        Assert.assertEquals(mockServer.findAll(WireMock.postRequestedFor(WireMock.anyUrl())).size(), 1,
                "[" + operation + "] a non-retryable error must not be retried");
    }

    /**
     * When retries are disabled ({@link ClientFaultCause#None}) even an otherwise retryable error
     * must be re-thrown on the first attempt. Covers the {@code else { throw lastException; }}
     * branch when {@code shouldRetry(...)} returns {@code false} because of the configuration.
     */
    @Test(groups = {"integration"}, dataProvider = "operationProvider")
    public void testRetriesDisabledThrowsImmediately(String operation,
                                                     ThrowingFunction<Client, Void> function) {
        if (isCloud()) {
            return; // mocked server
        }

        WireMockServer mockServer = startMockServer();

        mockServer.addStubMapping(WireMock.post(WireMock.anyUrl())
                .willReturn(WireMock.aResponse()
                        .withStatus(HttpStatus.SC_SERVICE_UNAVAILABLE)
                        .withHeader("X-ClickHouse-Exception-Code", String.valueOf(RETRYABLE_CODE))
                        .withBody(RETRYABLE_BODY)).build());

        try (Client client = mockServerClient(mockServer, 3, ClientFaultCause.None)) {
            function.apply(client);
            Assert.fail("[" + operation + "] expected exception when retries are disabled");
        } catch (ServerException e) {
            Assert.assertEquals(e.getCode(), RETRYABLE_CODE);
        } catch (Exception e) {
            Assert.fail("[" + operation + "] unexpected exception type", e);
        } finally {
            mockServer.stop();
        }

        Assert.assertEquals(mockServer.findAll(WireMock.postRequestedFor(WireMock.anyUrl())).size(), 1,
                "[" + operation + "] retries are disabled, no retry expected");
    }

    /**
     * When no query id is supplied by the caller but a generator is configured, the operation must
     * use the generated id. Covers the
     * {@code if (requestSettings.getQueryId() == null && queryIdGenerator != null)} block that is
     * present in every operation path.
     */
    @Test(groups = {"integration"}, dataProvider = "operationProvider")
    public void testGeneratedQueryIdIsUsedWhenNotSet(String operation,
                                                     ThrowingFunction<Client, Void> function) {
        if (isCloud()) {
            return; // mocked server
        }

        WireMockServer mockServer = startMockServer();
        mockServer.addStubMapping(WireMock.post(WireMock.anyUrl())
                .willReturn(WireMock.aResponse()
                        .withStatus(HttpStatus.SC_OK)
                        .withHeader("X-ClickHouse-Summary",
                                "{ \"read_bytes\": \"10\", \"read_rows\": \"1\"}")).build());

        String generatedId = "generated-" + UUID.randomUUID();
        AtomicInteger generatorCalls = new AtomicInteger();
        Supplier<String> idGenerator = () -> {
            generatorCalls.incrementAndGet();
            return generatedId;
        };

        try (Client client = new Client.Builder()
                .addEndpoint(Protocol.HTTP, "localhost", mockServer.port(), false) 
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .compressClientRequest(false)
                .compressServerResponse(false)
                .setQueryIdGenerator(idGenerator)
                .build()) {
            function.apply(client);
        } catch (Exception e) {
            mockServer.stop();
            Assert.fail("[" + operation + "] operation should succeed", e);
            return;
        }

        try {
            Assert.assertTrue(generatorCalls.get() >= 1,
                    "[" + operation + "] query id generator should have been invoked");
            mockServer.verify(WireMock.postRequestedFor(WireMock.anyUrl())
                    .withQueryParam("query_id", WireMock.equalTo(generatedId)));
        } finally {
            mockServer.stop();
        }
    }

    /**
     * Provides one {@link ThrowingFunction} per retry-loop code path in {@link Client}, so every
     * failure-handling test exercises all of them:
     * <ul>
     *     <li>{@code query} - {@link Client#query(String)}</li>
     *     <li>{@code insert-stream} - {@link Client#insert(String, java.io.InputStream, ClickHouseFormat)}</li>
     *     <li>{@code insert-pojo} - {@link Client#insert(String, java.util.List)}</li>
     * </ul>
     */
    @DataProvider(name = "operationProvider")
    public static Object[][] operationProvider() {
        ThrowingFunction<Client, Void> queryFunction = (client) -> {
            try (QueryResponse response = client.query("SELECT timezone()").get(30, TimeUnit.SECONDS)) {
                return null;
            }
        };

        ThrowingFunction<Client, Void> streamInsertFunction = (client) -> {
            try (InsertResponse response = client.insert("table01",
                    new ByteArrayInputStream("1\t2\t3\n".getBytes()), ClickHouseFormat.TSV)
                    .get(30, TimeUnit.SECONDS)) {
                return null;
            }
        };

        ThrowingFunction<Client, Void> pojoInsertFunction = (client) -> {
            client.register(InsertablePojo.class, new TableSchema("table01", null, "default",
                    Collections.singletonList(ClickHouseColumn.of("id", "Int32"))));
            try (InsertResponse response = client.insert("table01",
                    Collections.singletonList(new InsertablePojo(1)))
                    .get(30, TimeUnit.SECONDS)) {
                return null;
            }
        };

        return new Object[][]{
                {"query", queryFunction},
                {"insert-stream", streamInsertFunction},
                {"insert-pojo", pojoInsertFunction}
        };
    }

    /**
     * Exercises {@link Client#cancelTransportRequest(String)} for every combination of
     * {@code {query, insert} x {sync, async}} operations. A long-running operation is started against the
     * real server with a known {@code query_id} and is interrupted from the test thread while it is still
     * in progress. The whole life cycle is verified:
     * <ol>
     *     <li>the operation is observed running on the server ({@code system.processes});</li>
     *     <li>{@link Client#cancelTransportRequest(String)} aborts the in-flight request and the operation fails;</li>
     *     <li>the query is no longer running on the server.</li>
     * </ol>
     */
    @Test(groups = {"integration"}, dataProvider = "cancelRequestProvider")
    @SuppressWarnings("java:S2925")
    public void testCancelRequest(String name, boolean async, boolean isInsert) throws Exception {
        if (isCloud()) {
            return; // relies on local system tables (processes / query_log)
        }

        ClickHouseNode server = getServer(ClickHouseProtocol.HTTP);
        String queryId = "client-cancel-" + UUID.randomUUID();
        // Fully qualified so the table created via runQuery (default database) matches the one the client
        // inserts into (client default database is the test database).
        String table = ClickHouseServerForTest.getDatabase() + ".client_cancel_"
                + UUID.randomUUID().toString().replace('-', '_');

        try (Client client = new Client.Builder()
                .addEndpoint(Protocol.HTTP, server.getHost(), server.getPort(), false)
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .setDefaultDatabase(ClickHouseServerForTest.getDatabase())
                .compressClientRequest(false)
                .compressServerResponse(false)
                .useAsyncRequests(async)
                .build()) {

            AtomicReference<Throwable> opError = new AtomicReference<>();
            AtomicBoolean opFinished = new AtomicBoolean(false);

            Runnable operation;
            if (isInsert) {
                Assert.assertTrue(runQuery("CREATE TABLE " + table +
                        " (number UInt64) ENGINE = MergeTree ORDER BY number"), "[" + name + "] failed to create table");
                operation = () -> {
                    // Endless input stream so the insert stays active on the server until the request is cancelled.
                    try (InsertResponse response = client.insert(table, endlessTsvStream(), ClickHouseFormat.TSV,
                            new InsertSettings().setQueryId(queryId)).get(35, TimeUnit.SECONDS)) {
                        opFinished.set(true);
                    } catch (Throwable t) {
                        opError.set(t);
                    }
                };
            } else {
                operation = () -> {
                    // A long-running aggregation that produces no output until it finishes: the server sends no
                    // response while it runs, so the worker stays blocked in query().get() waiting for the
                    // response. That IO wait is exactly what cancelTransportRequest is meant to unblock (once the
                    // response has been received the request is no longer registered / cancellable).
                    try (QueryResponse response = client.query(
                            "SELECT sum(sleepEachRow(1)) FROM numbers(3600) SETTINGS max_block_size = 1",
                            new QuerySettings().setQueryId(queryId)).get(35, TimeUnit.SECONDS)) {
                        opFinished.set(true);
                    } catch (Throwable t) {
                        opError.set(t);
                    }
                };
            }

            Thread worker = new Thread(operation, "client-cancel-" + name);
            worker.start();

            // 1. The operation must actually be running on the server.
            Assert.assertTrue(waitForCondition(() -> isQueryRunning(client, queryId), 20_000),
                    "[" + name + "] operation was not observed running on the server (query_id=" + queryId
                            + ", opError=" + opError.get() + ")");
            Assert.assertNull(opError.get(), "[" + name + "] operation should still be in progress while it runs");

            // 2. Cancel the in-flight request through the high-level Client API. Cancellation is reissued in a
            // short loop so it reliably lands while the operation is still in progress, independent of thread
            // scheduling races between the worker and the canceller.
            cancelUntilStopped(client, queryId, worker, 20_000);
            Assert.assertFalse(worker.isAlive(), "[" + name + "] operation must stop after the request was cancelled");
            Assert.assertFalse(opFinished.get(), "[" + name + "] a cancelled operation must not complete successfully");
            Assert.assertNotNull(opError.get(), "[" + name + "] a cancelled operation must fail with an error");
        } finally {
            runQuery("DROP TABLE IF EXISTS " + table);
        }
    }

    @DataProvider(name = "cancelRequestProvider")
    public static Object[][] cancelRequestProvider() {
        return new Object[][]{
                {"query-sync", false, false},
                {"query-async", true, false},
                {"insert-sync", false, true},
                {"insert-async", true, true}
        };
    }

    /**
     * A retryable transport failure - here a socket read timeout produced by a mocked server that delays its
     * response well beyond the client socket timeout - is normally retried up to {@code maxRetries} times.
     * When the caller cancels the in-flight request through {@link Client#cancelTransportRequest(String)} while
     * the operation is inside that retry loop, the loop must stop early instead of exhausting the configured
     * retries. Covers the {@code requestIsNotCancelled(queryId)} guard of the operation retry loop: the
     * operation fails and the mocked server observes fewer than {@code maxRetries + 1} attempts.
     */
    @Test(groups = {"integration"}, dataProvider = "cancelDuringRetryProvider")
    @SuppressWarnings("java:S2925")
    public void testCancelStopsRetriesOnRetryableDelay(String operation, boolean isInsert, boolean isPojo)
            throws Exception {
        if (isCloud()) {
            return; // mocked server
        }

        int maxRetries = 50;
        WireMockServer mockServer = startMockServer();
        // The response is delayed far beyond the client socket timeout, so every attempt fails with a
        // retryable SocketTimeoutException and the operation stays in the retry loop until it is cancelled.
        mockServer.addStubMapping(WireMock.post(WireMock.anyUrl())
                .willReturn(WireMock.aResponse()
                        .withStatus(HttpStatus.SC_OK)
                        .withFixedDelay(5_000)
                        .withHeader("X-ClickHouse-Summary",
                                "{ \"read_bytes\": \"10\", \"read_rows\": \"1\"}")).build());

        String queryId = "cancel-retry-" + UUID.randomUUID();
        AtomicReference<Throwable> opError = new AtomicReference<>();
        AtomicBoolean opFinished = new AtomicBoolean(false);

        try (Client client = new Client.Builder()
                .addEndpoint(Protocol.HTTP, "localhost", mockServer.port(), false)
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .compressClientRequest(false)
                .compressServerResponse(false)
                .setSocketTimeout(500)
                .setMaxRetries(maxRetries)
                .retryOnFailures(ClientFaultCause.SocketTimeout)
                .build()) {

            Runnable op;
            if (isInsert && isPojo) {
                client.register(InsertablePojo.class, new TableSchema("table01", null, "default",
                        Collections.singletonList(ClickHouseColumn.of("id", "Int32"))));
                op = () -> {
                    try (InsertResponse response = client.insert("table01",
                            Collections.singletonList(new InsertablePojo(1)),
                            new InsertSettings().setQueryId(queryId)).get(60, TimeUnit.SECONDS)) {
                        opFinished.set(true);
                    } catch (Throwable t) {
                        opError.set(t);
                    }
                };
            } else if (isInsert) {
                op = () -> {
                    try (InsertResponse response = client.insert("table01",
                            new ByteArrayInputStream("1\t2\t3\n".getBytes()), ClickHouseFormat.TSV,
                            new InsertSettings().setQueryId(queryId)).get(60, TimeUnit.SECONDS)) {
                        opFinished.set(true);
                    } catch (Throwable t) {
                        opError.set(t);
                    }
                };
            } else {
                op = () -> {
                    try (QueryResponse response = client.query("SELECT timezone()",
                            new QuerySettings().setQueryId(queryId)).get(60, TimeUnit.SECONDS)) {
                        opFinished.set(true);
                    } catch (Throwable t) {
                        opError.set(t);
                    }
                };
            }

            Thread worker = new Thread(op, "cancel-retry-" + operation);
            worker.start();

            // Cancellation is reissued until the worker stops so it reliably lands while a request is in flight,
            // independent of scheduling races between the worker and the canceller.
            cancelUntilStopped(client, queryId, worker, 30_000);

            Assert.assertFalse(worker.isAlive(),
                    "[" + operation + "] operation must stop once the request is cancelled");
            Assert.assertFalse(opFinished.get(),
                    "[" + operation + "] a cancelled operation must not complete successfully");
            Assert.assertNotNull(opError.get(),
                    "[" + operation + "] a cancelled operation must fail with an error");

            int attempts = mockServer.findAll(WireMock.postRequestedFor(WireMock.anyUrl())).size();
            Assert.assertTrue(attempts < maxRetries + 1,
                    "[" + operation + "] cancellation must stop the retry loop early, but observed " + attempts
                            + " attempts (maxRetries=" + maxRetries + ")");
        } finally {
            mockServer.stop();
        }
    }

    @DataProvider(name = "cancelDuringRetryProvider")
    public static Object[][] cancelDuringRetryProvider() {
        return new Object[][]{
                {"query", false, false},
                {"insert-stream", true, false},
                {"insert-pojo", true, true}
        };
    }

    /**
     * Reissues {@link Client#cancelTransportRequest(String)} until the worker stops or the timeout elapses.
     * Retrying makes the test robust against thread-scheduling races where a single cancel could land before
     * the worker has actually started blocking on the request.
     */
    private static void cancelUntilStopped(Client client, String queryId, Thread worker, long timeoutMillis)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        do {
            client.cancelTransportRequest(queryId);
            worker.join(200);
        } while (worker.isAlive() && System.currentTimeMillis() < deadline);
    }

    /**
     * Produces an effectively endless stream of well-formed single-column TSV rows in 64 KiB chunks. The chunk
     * size is large enough to flush through the client's network buffer so the insert is actually observed
     * running on the server, while the short sleep keeps the request in progress without flooding it.
     */
    private static InputStream endlessTsvStream() {
        final byte[] row = "1\n".getBytes(StandardCharsets.US_ASCII);
        final byte[] chunk = new byte[64 * 1024];
        for (int i = 0; i < chunk.length; i++) {
            chunk[i] = row[i % row.length];
        }
        return new InputStream() {
            @Override
            public int read() {
                byte[] single = new byte[1];
                return read(single, 0, 1) == -1 ? -1 : (single[0] & 0xFF);
            }

            @Override
            public int read(byte[] b, int off, int len) {
                try {
                    Thread.sleep(20);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return -1;
                }
                int n = Math.min(len, chunk.length);
                System.arraycopy(chunk, 0, b, off, n);
                return n;
            }
        };
    }

    private static boolean isQueryRunning(Client client, String queryId) {
        List<GenericRecord> rows = client.queryAll(
                "SELECT count() AS c FROM system.processes WHERE query_id = '" + queryId + "'");
        return !rows.isEmpty() && rows.get(0).getLong("c") > 0;
    }

    private static boolean waitForCondition(Supplier<Boolean> condition, long timeoutMillis)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            try {
                if (Boolean.TRUE.equals(condition.get())) {
                    return true;
                }
            } catch (Exception ignore) {
                // transient query failures (e.g. server busy) are retried until the timeout elapses
            }
            Thread.sleep(100);
        }
        return false;
    }

    public static class InsertablePojo {
        private int id;

        public InsertablePojo() {
        }

        public InsertablePojo(int id) {
            this.id = id;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }
    }
}

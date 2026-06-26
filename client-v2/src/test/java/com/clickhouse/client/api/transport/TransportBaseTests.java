package com.clickhouse.client.api.transport;


import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientFaultCause;
import com.clickhouse.client.api.ServerException;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.QueryResponse;
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
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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

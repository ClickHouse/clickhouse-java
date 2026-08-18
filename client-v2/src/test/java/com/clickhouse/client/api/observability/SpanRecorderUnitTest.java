package com.clickhouse.client.api.observability;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.observability.CapturingSpanRecorder.CapturedSpan;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseFormat;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class SpanRecorderUnitTest {

    private static final String DEAD_ENDPOINT = "http://127.0.0.1:1"; // nothing listens here

    private CapturingSpanRecorder recorder;
    private WireMockServer mockServer;

    @BeforeMethod
    void setUp() {
        recorder = new CapturingSpanRecorder();
        mockServer = new WireMockServer(WireMockConfiguration.options().dynamicPort());
        mockServer.start();
        mockServer.stubFor(WireMock.post(WireMock.anyUrl())
                .willReturn(WireMock.aResponse().withStatus(200)
                        .withHeader("Content-Type", "text/plain")
                        .withBody("")));
    }

    @AfterMethod
    void tearDown() {
        mockServer.stop();
    }

    @Test
    public void testQuerySpanReportsStatementAndParameters() throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("id", 42);
        params.put("phrase", "hello");

        try (Client client = newClientBuilder().addEndpoint(mockEndpoint()).build()) {
            QuerySettings settings = new QuerySettings().setQueryId("query-id-1");
            try (QueryResponse response = client.query("SELECT {id:UInt8}, {phrase:String}", params, settings)
                    .get(10, TimeUnit.SECONDS)) {
                Assert.assertNotNull(response);
            }
        }

        CapturedSpan operationSpan = recorder.operationSpan();
        Assert.assertEquals(operationSpan.getName(), "query test_db");
        Assert.assertEquals(operationSpan.getSettingsDatabase(), "test_db",
                "the recorder must receive the resolved operation settings");
        Assert.assertEquals(operationSpan.getSettingsQueryId(), "query-id-1");
        Assert.assertEquals(operationSpan.getAttribute(SpanAttribute.DB_SYSTEM_NAME), "clickhouse");
        Assert.assertEquals(operationSpan.getAttribute(SpanAttribute.DB_NAMESPACE), "test_db");
        Assert.assertEquals(operationSpan.getAttribute(SpanAttribute.DB_QUERY_TEXT),
                "SELECT {id:UInt8}, {phrase:String}");
        Assert.assertEquals(operationSpan.getAttribute(SpanAttribute.CLICKHOUSE_QUERY_ID), "query-id-1");
        Assert.assertEquals(operationSpan.getAttribute(SpanAttribute.DB_QUERY_PARAMETER.getKey("id")), "42");
        Assert.assertEquals(operationSpan.getAttribute(SpanAttribute.DB_QUERY_PARAMETER.getKey("phrase")), "hello");
        Assert.assertEquals(operationSpan.getAttribute(SpanAttribute.SERVER_ADDRESS), "localhost");
        Assert.assertEquals(operationSpan.getAttribute(SpanAttribute.SERVER_PORT), mockServer.port());
        Assert.assertNull(operationSpan.getAttribute(SpanAttribute.DB_OPERATION_NAME),
                "a plain query has no operation name");
        Assert.assertNull(operationSpan.getAttribute(SpanAttribute.DB_COLLECTION_NAME));
        Assert.assertNull(operationSpan.getErrorType());
        Assert.assertEquals(operationSpan.getEndCount(), 1);

        List<CapturedSpan> requestSpans = recorder.requestSpans(operationSpan);
        Assert.assertEquals(requestSpans.size(), 1);
        CapturedSpan requestSpan = requestSpans.get(0);
        Assert.assertEquals(requestSpan.getName(), "POST");
        Assert.assertEquals(requestSpan.getAttribute(SpanAttribute.HTTP_REQUEST_METHOD), "POST");
        Assert.assertEquals(requestSpan.getAttribute(SpanAttribute.HTTP_RESPONSE_STATUS_CODE), 200);
        Assert.assertEquals(requestSpan.getAttribute(SpanAttribute.SERVER_ADDRESS), "localhost");
        Assert.assertEquals(requestSpan.getAttribute(SpanAttribute.SERVER_PORT), mockServer.port());
        Assert.assertNull(requestSpan.getErrorType());
        Assert.assertEquals(requestSpan.getEndCount(), 1);
    }

    @Test
    public void testPojoInsertSpanReportsBatchSize() throws Exception {
        try (Client client = newClientBuilder().addEndpoint(mockEndpoint()).build()) {
            client.register(ValuePojo.class, new TableSchema("target_table", null, "",
                    Collections.singletonList(ClickHouseColumn.of("value", "String"))));
            try (InsertResponse response = client.insert("target_table",
                    Arrays.asList(new ValuePojo("a"), new ValuePojo("b"), new ValuePojo("c")))
                    .get(10, TimeUnit.SECONDS)) {
                Assert.assertNotNull(response);
            }
        }

        CapturedSpan operationSpan = recorder.operationSpan();
        Assert.assertEquals(operationSpan.getName(), "insert test_db.target_table");
        Assert.assertEquals(operationSpan.getAttribute(SpanAttribute.DB_OPERATION_NAME), "insert");
        Assert.assertEquals(operationSpan.getAttribute(SpanAttribute.DB_COLLECTION_NAME), "target_table");
        Assert.assertEquals(operationSpan.getAttribute(SpanAttribute.DB_NAMESPACE), "test_db");
        Assert.assertEquals(operationSpan.getAttribute(SpanAttribute.DB_OPERATION_BATCH_SIZE), 3);
        Assert.assertNull(operationSpan.getAttribute(SpanAttribute.DB_QUERY_TEXT),
                "an insert does not report a statement");
        Assert.assertEquals(operationSpan.getEndCount(), 1);
        Assert.assertEquals(recorder.requestSpans(operationSpan).size(), 1);
    }

    @Test
    public void testStreamInsertSpanHasNoBatchSize() throws Exception {
        try (Client client = newClientBuilder().addEndpoint(mockEndpoint()).build()) {
            try (InsertResponse response = client.insert("target_table",
                    new ByteArrayInputStream("value\n".getBytes(StandardCharsets.UTF_8)),
                    ClickHouseFormat.CSV, new InsertSettings()).get(10, TimeUnit.SECONDS)) {
                Assert.assertNotNull(response);
            }
        }

        CapturedSpan operationSpan = recorder.operationSpan();
        Assert.assertEquals(operationSpan.getName(), "insert test_db.target_table");
        Assert.assertEquals(operationSpan.getAttribute(SpanAttribute.DB_COLLECTION_NAME), "target_table");
        Assert.assertNull(operationSpan.getAttribute(SpanAttribute.DB_OPERATION_BATCH_SIZE),
                "the number of rows in a stream is not known to the client");
        Assert.assertEquals(operationSpan.getEndCount(), 1);
    }

    @DataProvider(name = "retryCounts")
    public static Object[][] retryCounts() {
        return new Object[][]{{0, 1}, {1, 2}, {3, 4}};
    }

    @Test(dataProvider = "retryCounts")
    public void testEveryAttemptIsRecordedAsChildRequestSpan(int maxRetries, int expectedAttempts) throws Exception {
        String expectedErrorType = null;
        try (Client client = newClientBuilder().addEndpoint(DEAD_ENDPOINT).setMaxRetries(maxRetries).build()) {
            try {
                client.query("SELECT 1").get(30, TimeUnit.SECONDS);
                Assert.fail("a query against a dead endpoint must fail");
            } catch (ExecutionException e) {
                expectedErrorType = e.getCause().getClass().getName();
            } catch (RuntimeException e) {
                // with synchronous operations the failure is thrown by the operation itself
                expectedErrorType = e.getClass().getName();
            }
        }

        CapturedSpan operationSpan = recorder.operationSpan();
        Assert.assertEquals(operationSpan.getErrorType(), expectedErrorType);
        Assert.assertEquals(operationSpan.getEndCount(), 1, "an operation span is ended exactly once");

        List<CapturedSpan> requestSpans = recorder.requestSpans(operationSpan);
        Assert.assertEquals(requestSpans.size(), expectedAttempts, "one request span per attempt");
        for (CapturedSpan requestSpan : requestSpans) {
            Assert.assertEquals(requestSpan.getAttribute(SpanAttribute.SERVER_ADDRESS), "127.0.0.1");
            Assert.assertEquals(requestSpan.getAttribute(SpanAttribute.SERVER_PORT), 1);
            Assert.assertNull(requestSpan.getAttribute(SpanAttribute.HTTP_RESPONSE_STATUS_CODE),
                    "no response was received");
            Assert.assertNotNull(requestSpan.getErrorType());
            Assert.assertEquals(requestSpan.getEndCount(), 1);
        }
    }

    @Test
    public void testFirstEndpointOnOperationSpanAndPerAttemptOnRequestSpans() throws Exception {
        try (Client client = newClientBuilder()
                .addEndpoint(DEAD_ENDPOINT)
                .addEndpoint(mockEndpoint())
                .setMaxRetries(3)
                .build()) {
            try (QueryResponse response = client.query("SELECT 1").get(30, TimeUnit.SECONDS)) {
                Assert.assertNotNull(response);
            }
        }

        CapturedSpan operationSpan = recorder.operationSpan();
        Assert.assertEquals(operationSpan.getAttribute(SpanAttribute.SERVER_ADDRESS), "127.0.0.1",
                "the operation reports the first configured endpoint; every attempt reports its own");
        Assert.assertEquals(operationSpan.getAttribute(SpanAttribute.SERVER_PORT), 1);
        Assert.assertNull(operationSpan.getErrorType());

        List<CapturedSpan> requestSpans = recorder.requestSpans(operationSpan);
        Assert.assertFalse(requestSpans.isEmpty());
        for (CapturedSpan requestSpan : requestSpans) {
            Assert.assertNotNull(requestSpan.getAttribute(SpanAttribute.SERVER_ADDRESS));
            Assert.assertNotNull(requestSpan.getAttribute(SpanAttribute.SERVER_PORT));
        }
        CapturedSpan lastRequestSpan = requestSpans.get(requestSpans.size() - 1);
        Assert.assertEquals(lastRequestSpan.getAttribute(SpanAttribute.HTTP_RESPONSE_STATUS_CODE), 200);
        Assert.assertEquals(lastRequestSpan.getAttribute(SpanAttribute.SERVER_PORT), mockServer.port());
    }

    @Test
    public void testFailedInsertRecordsErrorOnOperationAndRequestSpans() throws Exception {
        String expectedErrorType = null;
        try (Client client = newClientBuilder().addEndpoint(DEAD_ENDPOINT).setMaxRetries(1).build()) {
            client.register(ValuePojo.class, new TableSchema("target_table", null, "",
                    Collections.singletonList(ClickHouseColumn.of("value", "String"))));
            try {
                client.insert("target_table", Collections.singletonList(new ValuePojo("a"))).get(30, TimeUnit.SECONDS);
                Assert.fail("an insert into a dead endpoint must fail");
            } catch (ExecutionException e) {
                expectedErrorType = e.getCause().getClass().getName();
            } catch (RuntimeException e) {
                expectedErrorType = e.getClass().getName();
            }
        }

        CapturedSpan operationSpan = recorder.operationSpan();
        Assert.assertEquals(operationSpan.getName(), "insert test_db.target_table");
        Assert.assertEquals(operationSpan.getErrorType(), expectedErrorType);
        Assert.assertEquals(operationSpan.getEndCount(), 1);

        List<CapturedSpan> requestSpans = recorder.requestSpans(operationSpan);
        Assert.assertEquals(requestSpans.size(), 2, "one request span per attempt");
        for (CapturedSpan requestSpan : requestSpans) {
            Assert.assertNotNull(requestSpan.getErrorType());
            Assert.assertEquals(requestSpan.getEndCount(), 1);
        }
    }

    @DataProvider(name = "mappedErrorStatuses")
    public static Object[][] mappedErrorStatuses() {
        // statuses the transport maps onto an exception that does not carry the HTTP status itself
        return new Object[][] {
                {407}, // proxy authentication required -> ClientMisconfigurationException
                {502}, // bad gateway -> ConnectException
                {503}, // service unavailable -> ConnectException
                {418}, // unknown status -> ClientException
        };
    }

    @Test(dataProvider = "mappedErrorStatuses")
    public void testRequestSpanReportsHttpStatusOfMappedErrorResponses(int status) throws Exception {
        // the request span reports the HTTP status of every response the server sent, also when the
        // transport maps that response onto an exception that does not carry the status
        mockServer.resetAll();
        mockServer.stubFor(WireMock.post(WireMock.anyUrl())
                .willReturn(WireMock.aResponse().withStatus(status)
                        .withHeader("Content-Type", "text/plain")
                        .withBody("")));

        try (Client client = newClientBuilder().addEndpoint(mockEndpoint()).setMaxRetries(0).build()) {
            try {
                client.query("SELECT 1").get(10, TimeUnit.SECONDS).close();
                Assert.fail("a query answered with status " + status + " must fail");
            } catch (ExecutionException | RuntimeException e) {
                // expected - the response is mapped onto a failure
            }
        }

        CapturedSpan operationSpan = recorder.operationSpan();
        Assert.assertNotNull(operationSpan.getErrorType(), "the failed operation must report an error type");

        List<CapturedSpan> requestSpans = recorder.requestSpans(operationSpan);
        Assert.assertEquals(requestSpans.size(), 1, "one request span per attempt");
        CapturedSpan requestSpan = requestSpans.get(0);
        Assert.assertEquals(requestSpan.getAttribute(SpanAttribute.HTTP_RESPONSE_STATUS_CODE), status,
                "the status of the received response must be reported on the request span");
        Assert.assertNotNull(requestSpan.getErrorType(), "the failed request must report an error type");
        Assert.assertEquals(requestSpan.getEndCount(), 1);
    }

    @Test
    public void testRequestSpanReportsNoHttpStatusWhenNoResponseArrives() throws Exception {
        // contrast case: without a response there is no status to report
        try (Client client = newClientBuilder().addEndpoint(DEAD_ENDPOINT).setMaxRetries(0).build()) {
            try {
                client.query("SELECT 1").get(30, TimeUnit.SECONDS).close();
                Assert.fail("a query to a dead endpoint must fail");
            } catch (ExecutionException | RuntimeException e) {
                // expected - nothing listens on the endpoint
            }
        }

        CapturedSpan operationSpan = recorder.operationSpan();
        List<CapturedSpan> requestSpans = recorder.requestSpans(operationSpan);
        Assert.assertEquals(requestSpans.size(), 1);
        CapturedSpan requestSpan = requestSpans.get(0);
        Assert.assertNull(requestSpan.getAttribute(SpanAttribute.HTTP_RESPONSE_STATUS_CODE),
                "no response arrived, so no HTTP status is reported");
        Assert.assertNotNull(requestSpan.getErrorType());
        Assert.assertEquals(requestSpan.getEndCount(), 1);
    }

    @Test
    public void testOperationSpanIsStartedOnCallingThreadWithAsyncRequests() throws Exception {
        Thread callingThread = Thread.currentThread();
        ThreadRecordingSpanRecorder threadRecorder = new ThreadRecordingSpanRecorder();
        try (Client client = new Client.Builder()
                .addEndpoint(mockEndpoint())
                .setUsername("default")
                .setPassword("")
                .setDefaultDatabase("test_db")
                .useAsyncRequests(true)
                .setSpanRecorder(threadRecorder)
                .build()) {
            try (QueryResponse response = client.query("SELECT 1").get(10, TimeUnit.SECONDS)) {
                Assert.assertNotNull(response);
            }
        }

        Assert.assertEquals(threadRecorder.operationSpan().getName(), "query test_db");
        Assert.assertSame(threadRecorder.operationStartThread, callingThread,
                "an operation span must be started on the caller's thread so it joins the caller's trace");
        Assert.assertNotSame(threadRecorder.requestStartThread, callingThread,
                "the request itself is executed on the operation executor");
        Assert.assertEquals(threadRecorder.operationSpan().getEndCount(), 1);
        Assert.assertEquals(threadRecorder.requestSpans(threadRecorder.operationSpan()).size(), 1);
    }

    @Test
    public void testNullRecorderIsRejected() {
        // the default recorder already records nothing, so a null recorder is a configuration error
        Assert.assertThrows(NullPointerException.class,
                () -> newClientBuilder().addEndpoint(mockEndpoint()).setSpanRecorder(null));
    }

    @Test
    public void testNothingIsRecordedWithTheDefaultRecorder() throws Exception {
        try (Client client = newClientBuilder().addEndpoint(mockEndpoint())
                .setSpanRecorder(DefaultSpanRecorder.NOOP).build()) {
            try (QueryResponse response = client.query("SELECT 1").get(10, TimeUnit.SECONDS)) {
                Assert.assertNotNull(response);
            }
        }

        Assert.assertTrue(recorder.getSpans().isEmpty(),
                "a recorder that records nothing replaces a previously set one and records nothing");
    }

    @Test
    public void testDefaultSpanRecorderRecordsNothing() {
        SpanRecorder defaultRecorder = new DefaultSpanRecorder();
        Assert.assertSame(defaultRecorder.startSpan("query", new QuerySettings()), DefaultSpanRecorder.NOOP_SPAN);
        Assert.assertSame(defaultRecorder.startSpan("insert", new InsertSettings()), DefaultSpanRecorder.NOOP_SPAN);
        Assert.assertSame(defaultRecorder.startRequestSpan("POST", DefaultSpanRecorder.NOOP_SPAN),
                DefaultSpanRecorder.NOOP_SPAN);

        Span noopSpan = DefaultSpanRecorder.NOOP_SPAN;
        noopSpan.setAttribute(SpanAttribute.DB_NAMESPACE.getKey(), "db");
        noopSpan.setError("java.lang.IllegalStateException");
        noopSpan.end();
    }

    @Test
    public void testRecorderMayRecordQuerySpansOnly() throws Exception {
        // a recorder that overrides one kind of span only must keep working - the kinds it does not
        // override are answered by the base class with a span that records nothing
        QueryOnlySpanRecorder queryOnlyRecorder = new QueryOnlySpanRecorder();
        try (Client client = new Client.Builder()
                .setUsername("default")
                .setPassword("")
                .setDefaultDatabase("test_db")
                .setSpanRecorder(queryOnlyRecorder)
                .addEndpoint(mockEndpoint())
                .build()) {
            try (QueryResponse response = client.query("SELECT 1").get(10, TimeUnit.SECONDS)) {
                Assert.assertNotNull(response);
            }
            client.insert("t1", new ByteArrayInputStream("1\n".getBytes(StandardCharsets.UTF_8)),
                    ClickHouseFormat.TSV).get(10, TimeUnit.SECONDS).close();
        }

        Assert.assertEquals(queryOnlyRecorder.querySpans.size(), 1,
                "the overridden method is the only one that recorded a span");
        CapturedSpan querySpan = queryOnlyRecorder.querySpans.get(0);
        Assert.assertEquals(querySpan.getName(), "query test_db");
        Assert.assertEquals(querySpan.getAttribute(SpanAttribute.DB_QUERY_TEXT), "SELECT 1");
        Assert.assertEquals(querySpan.getEndCount(), 1);
    }

    @Test
    public void testRecorderReturningNullSpansDoesNotBreakOperations() throws Exception {
        SpanRecorder nullRecorder = new SpanRecorder() {
            @Override
            public Span startSpan(String spanName, QuerySettings settings) {
                return null;
            }

            @Override
            public Span startSpan(String spanName, InsertSettings settings) {
                return null;
            }

            @Override
            public Span startRequestSpan(String spanName, Span operationSpan) {
                return null;
            }
        };

        try (Client client = new Client.Builder()
                .setUsername("default")
                .setPassword("")
                .setDefaultDatabase("test_db")
                .setSpanRecorder(nullRecorder)
                .addEndpoint(mockEndpoint())
                .build()) {
            try (QueryResponse response = client.query("SELECT 1").get(10, TimeUnit.SECONDS)) {
                Assert.assertNotNull(response, "a recorder that returns no span must not fail the operation");
            }
        }
    }

    /**
     * Recorder that implements the query span only and inherits everything else from the base class.
     */
    private static class QueryOnlySpanRecorder extends DefaultSpanRecorder {
        final List<CapturedSpan> querySpans = Collections.synchronizedList(new ArrayList<>());

        @Override
        public Span startSpan(String spanName, QuerySettings settings) {
            CapturedSpan span = new CapturedSpan(spanName, null, settings.getDatabase(), settings.getQueryId());
            querySpans.add(span);
            return span;
        }
    }

    private static class ThreadRecordingSpanRecorder extends CapturingSpanRecorder {
        volatile Thread operationStartThread;
        volatile Thread requestStartThread;

        @Override
        public Span startSpan(String spanName, QuerySettings settings) {
            operationStartThread = Thread.currentThread();
            return super.startSpan(spanName, settings);
        }

        @Override
        public Span startRequestSpan(String spanName, Span operationSpan) {
            requestStartThread = Thread.currentThread();
            return super.startRequestSpan(spanName, operationSpan);
        }
    }

    private Client.Builder newClientBuilder() {
        return new Client.Builder()
                .setUsername("default")
                .setPassword("")
                .setDefaultDatabase("test_db")
                .setSpanRecorder(recorder);
    }

    private String mockEndpoint() {
        return "http://localhost:" + mockServer.port();
    }

    public static class ValuePojo {
        private String value;

        public ValuePojo() {
        }

        public ValuePojo(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }
}

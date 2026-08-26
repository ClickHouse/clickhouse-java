package com.clickhouse.client.api.observability;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.observability.CapturingMetricsRecorder.RecordedMetric;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.client.api.transport.Endpoint;
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
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class MetricsRecorderUnitTest {

    private static final String DEAD_ENDPOINT = "http://127.0.0.1:1"; // nothing listens here

    // Work a span recorder does before the operation runs and after it failed. The two delays differ so that
    // each end of the measured interval is pinned on its own, and both are long enough to stand out from the
    // operation itself, which fails as soon as the connection to the dead endpoint is refused.
    private static final Duration SPAN_START_DELAY = Duration.ofMillis(800);
    private static final Duration RECORD_FAILURE_DELAY = Duration.ofMillis(500);

    private CapturingMetricsRecorder recorder;
    private WireMockServer mockServer;

    @BeforeMethod
    void setUp() {
        recorder = new CapturingMetricsRecorder();
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
    public void testQueryReportsDurationAndOperationAttributes() throws Exception {
        try (Client client = newClientBuilder().addEndpoint(mockEndpoint()).build()) {
            try (QueryResponse response = client.query("SELECT 1", new QuerySettings().setQueryId("query-id-1"))
                    .get(10, TimeUnit.SECONDS)) {
                Assert.assertNotNull(response);
            }
        }

        RecordedMetric duration = recorder.getOnlyMetric(MetricName.OPERATION_DURATION);
        Assert.assertTrue(duration.getValue() > 0, "Unexpected duration: " + duration.getValue());
        Assert.assertEquals(duration.getAttribute(MetricAttribute.DB_SYSTEM_NAME), "clickhouse");
        Assert.assertEquals(duration.getAttribute(MetricAttribute.DB_NAMESPACE), "test_db");
        Assert.assertEquals(duration.getAttribute(MetricAttribute.DB_OPERATION_NAME), "query");
        Assert.assertNull(duration.getAttribute(MetricAttribute.ERROR_TYPE), "the operation succeeded");
        Assert.assertNull(duration.getAttribute(MetricAttribute.DB_COLLECTION_NAME), "a query has no table");
        Assert.assertEquals(recorder.getOnlyMetric(MetricName.OPERATION_COUNT).getValue(), 1d);
        Assert.assertTrue(recorder.getMetrics(MetricName.OPERATION_RETRIES).isEmpty(), "no attempt failed");
    }

    @Test
    public void testInsertReportsTableAndSerializationDuration() throws Exception {
        try (Client client = newClientBuilder().addEndpoint(mockEndpoint()).build()) {
            client.register(ValuePojo.class, new TableSchema("target_table", null, "",
                    Collections.singletonList(ClickHouseColumn.of("value", "String"))));
            client.insert("target_table", Collections.singletonList(new ValuePojo("a"))).get(10, TimeUnit.SECONDS)
                    .close();
        }

        RecordedMetric duration = recorder.getOnlyMetric(MetricName.OPERATION_DURATION);
        Assert.assertTrue(duration.getValue() > 0, "Unexpected duration: " + duration.getValue());
        Assert.assertEquals(duration.getAttribute(MetricAttribute.DB_OPERATION_NAME), "insert");
        Assert.assertEquals(duration.getAttribute(MetricAttribute.DB_COLLECTION_NAME), "target_table");
        Assert.assertNull(duration.getAttribute(MetricAttribute.ERROR_TYPE));

        RecordedMetric serialization = recorder.getOnlyMetric(MetricName.OPERATION_SERIALIZATION_DURATION);
        Assert.assertTrue(serialization.getValue() > 0, "Unexpected duration: " + serialization.getValue());
        Assert.assertEquals(serialization.getAttribute(MetricAttribute.DB_COLLECTION_NAME), "target_table");
    }

    @Test
    public void testQueryWithoutSerializationStepReportsNoSerializationDuration() throws Exception {
        // contrast case: the client measures the serialization step of an insert only
        try (Client client = newClientBuilder().addEndpoint(mockEndpoint()).build()) {
            client.query("SELECT 1").get(10, TimeUnit.SECONDS).close();
        }

        Assert.assertTrue(recorder.getMetrics(MetricName.OPERATION_SERIALIZATION_DURATION).isEmpty(),
                "a duration the client did not measure must not be reported");
    }

    @Test
    public void testFailedQueryIsCountedWithErrorType() throws Exception {
        String expectedErrorType = null;
        try (Client client = newClientBuilder().addEndpoint(DEAD_ENDPOINT).setMaxRetries(0).build()) {
            try {
                client.query("SELECT 1").get(30, TimeUnit.SECONDS).close();
                Assert.fail("a query against a dead endpoint must fail");
            } catch (ExecutionException e) {
                expectedErrorType = e.getCause().getClass().getName();
            } catch (RuntimeException e) {
                // with synchronous operations the failure is thrown by the operation itself
                expectedErrorType = e.getClass().getName();
            }
        }

        RecordedMetric count = recorder.getOnlyMetric(MetricName.OPERATION_COUNT);
        Assert.assertEquals(count.getValue(), 1d, "a failed operation is counted too");
        Assert.assertEquals(count.getAttribute(MetricAttribute.ERROR_TYPE), expectedErrorType);
        Assert.assertEquals(count.getAttribute(MetricAttribute.DB_OPERATION_NAME), "query");

        RecordedMetric duration = recorder.getOnlyMetric(MetricName.OPERATION_DURATION);
        Assert.assertTrue(duration.getValue() > 0, "the client measures the duration of a failed operation");
        Assert.assertEquals(duration.getAttribute(MetricAttribute.ERROR_TYPE), expectedErrorType);
    }

    @Test
    public void testFailedInsertIsCountedWithTableAndErrorType() throws Exception {
        String expectedErrorType = null;
        try (Client client = newClientBuilder().addEndpoint(DEAD_ENDPOINT).setMaxRetries(0).build()) {
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

        RecordedMetric count = recorder.getOnlyMetric(MetricName.OPERATION_COUNT);
        Assert.assertEquals(count.getValue(), 1d);
        Assert.assertEquals(count.getAttribute(MetricAttribute.DB_OPERATION_NAME), "insert");
        Assert.assertEquals(count.getAttribute(MetricAttribute.DB_COLLECTION_NAME), "target_table");
        Assert.assertEquals(count.getAttribute(MetricAttribute.ERROR_TYPE), expectedErrorType);
    }

    @Test
    public void testStreamInsertReportsTargetTable() throws Exception {
        try (Client client = newClientBuilder().addEndpoint(mockEndpoint()).build()) {
            client.insert("target_table", new ByteArrayInputStream("a\n".getBytes(StandardCharsets.UTF_8)),
                    ClickHouseFormat.TabSeparated).get(10, TimeUnit.SECONDS).close();
        }

        RecordedMetric duration = recorder.getOnlyMetric(MetricName.OPERATION_DURATION);
        Assert.assertTrue(duration.getValue() > 0, "Unexpected duration: " + duration.getValue());
        Assert.assertEquals(duration.getAttribute(MetricAttribute.DB_OPERATION_NAME), "insert");
        Assert.assertEquals(duration.getAttribute(MetricAttribute.DB_COLLECTION_NAME), "target_table");
        Assert.assertEquals(recorder.getOnlyMetric(MetricName.OPERATION_COUNT).getValue(), 1d);
    }

    @Test
    public void testEveryRetriedInsertAttemptIsReported() throws Exception {
        try (Client client = newClientBuilder().addEndpoint(DEAD_ENDPOINT).setMaxRetries(2).build()) {
            try {
                client.insert("target_table", new ByteArrayInputStream("a\n".getBytes(StandardCharsets.UTF_8)),
                        ClickHouseFormat.TabSeparated).get(30, TimeUnit.SECONDS).close();
                Assert.fail("an insert into a dead endpoint must fail");
            } catch (ExecutionException | RuntimeException e) {
                // expected - nothing listens on the endpoint
            }
        }

        List<RecordedMetric> retries = recorder.getMetrics(MetricName.OPERATION_RETRIES);
        Assert.assertEquals(retries.size(), 2, "one retry event per retried attempt");
        RecordedMetric failure = recorder.getOnlyMetric(MetricName.OPERATION_COUNT);
        for (RecordedMetric retry : retries) {
            Assert.assertEquals(retry.getAttribute(MetricAttribute.DB_COLLECTION_NAME), "target_table");
            Assert.assertEquals(retry.getAttribute(MetricAttribute.ERROR_TYPE),
                    failure.getAttribute(MetricAttribute.ERROR_TYPE),
                    "a retry reports the failure the way the operation reports it");
        }
    }

    @DataProvider(name = "retryCounts")
    public static Object[][] retryCounts() {
        return new Object[][]{{0}, {1}, {3}};
    }

    @Test(dataProvider = "retryCounts")
    public void testEveryRetriedAttemptIsReported(int maxRetries) throws Exception {
        try (Client client = newClientBuilder().addEndpoint(DEAD_ENDPOINT).setMaxRetries(maxRetries).build()) {
            try {
                client.query("SELECT 1").get(30, TimeUnit.SECONDS).close();
                Assert.fail("a query against a dead endpoint must fail");
            } catch (ExecutionException | RuntimeException e) {
                // expected - nothing listens on the endpoint
            }
        }

        List<RecordedMetric> retries = recorder.getMetrics(MetricName.OPERATION_RETRIES);
        Assert.assertEquals(retries.size(), maxRetries, "one retry event per retried attempt");
        for (RecordedMetric retry : retries) {
            Assert.assertEquals(retry.getValue(), 1d);
            Assert.assertEquals(retry.getAttribute(MetricAttribute.DB_OPERATION_NAME), "query");
            Assert.assertNotNull(retry.getAttribute(MetricAttribute.ERROR_TYPE),
                    "a retry reports what made the attempt fail");
        }
        Assert.assertEquals(recorder.getMetrics(MetricName.OPERATION_COUNT).size(), 1,
                "a retried operation is still one operation");
    }

    @Test
    public void testFailedQueryIsMeasuredFromTheSameOriginAsASuccessfulOne() throws Exception {
        try (Client client = newClientBuilder().addEndpoint(DEAD_ENDPOINT).setMaxRetries(0)
                .setSpanRecorder(new DelayingSpanRecorder()).build()) {
            Assert.assertThrows(Exception.class,
                    () -> client.query("SELECT 1").get(30, TimeUnit.SECONDS).close());
        }

        assertFailureDurationOrigin(recorder.getOnlyMetric(MetricName.OPERATION_DURATION).getValue());
    }

    @Test
    public void testFailedInsertIsMeasuredFromTheSameOriginAsASuccessfulOne() throws Exception {
        try (Client client = newClientBuilder().addEndpoint(DEAD_ENDPOINT).setMaxRetries(0)
                .setSpanRecorder(new DelayingSpanRecorder()).build()) {
            client.register(ValuePojo.class, new TableSchema("target_table", null, "",
                    Collections.singletonList(ClickHouseColumn.of("value", "String"))));
            Assert.assertThrows(Exception.class, () -> client
                    .insert("target_table", Collections.singletonList(new ValuePojo("a"))).get(30, TimeUnit.SECONDS));
        }

        assertFailureDurationOrigin(recorder.getOnlyMetric(MetricName.OPERATION_DURATION).getValue());
    }

    /**
     * The client starts the duration of a successful operation before it prepares the request and before it
     * starts the operation span, and stops it before any recorder runs. A failed operation must report a
     * duration that covers the same work, so that both outcomes form one latency series.
     */
    private static void assertFailureDurationOrigin(double reportedSeconds) {
        double spanStartSeconds = SPAN_START_DELAY.toMillis() / 1000d;
        double recordFailureSeconds = RECORD_FAILURE_DELAY.toMillis() / 1000d;
        Assert.assertTrue(reportedSeconds >= spanStartSeconds,
                "the duration must start where the client starts the duration of a successful operation, so that it "
                        + "covers the work done before the operation span starts; reported: " + reportedSeconds);
        Assert.assertTrue(reportedSeconds < spanStartSeconds + recordFailureSeconds,
                "the duration must be taken before any recorder runs, like the duration of a successful operation; "
                        + "reported: " + reportedSeconds);
    }

    /**
     * Span recorder that spends a known amount of time when the client starts an operation span and again
     * when it reports a failure - the two points that fence the origin of a failed operation's duration.
     */
    private static final class DelayingSpanRecorder extends DefaultSpanRecorder {

        @Override
        public Span startQuerySpan(QuerySettings settings, String sqlQuery, Endpoint endpoint) {
            sleep(SPAN_START_DELAY);
            return super.startQuerySpan(settings, sqlQuery, endpoint);
        }

        @Override
        public Span startInsertSpan(InsertSettings settings, String tableName, int batchSize, Endpoint endpoint) {
            sleep(SPAN_START_DELAY);
            return super.startInsertSpan(settings, tableName, batchSize, endpoint);
        }

        @Override
        public void recordFailure(Span operationSpan, Throwable t) {
            sleep(RECORD_FAILURE_DELAY);
        }

        private static void sleep(Duration delay) {
            try {
                Thread.sleep(delay.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
    }

    @Test
    public void testAttributesOfASuccessfulOperationAreLowCardinality() {
        QuerySettings settings = new QuerySettings().setDatabase("test_db").setQueryId("query-id-1");

        Map<String, Object> attributes = MetricsSupport.DEFAULT.queryAttributes(settings, null);

        Assert.assertEquals(attributes.get(MetricAttribute.DB_SYSTEM_NAME.getKey()), "clickhouse");
        Assert.assertEquals(attributes.get(MetricAttribute.DB_NAMESPACE.getKey()), "test_db");
        Assert.assertEquals(attributes.get(MetricAttribute.DB_OPERATION_NAME.getKey()), "query");
        Assert.assertEquals(attributes.size(), 3,
                "the query id and the statement are recorded on spans, not on metrics: " + attributes);
    }

    @DataProvider(name = "unmeasuredDurations")
    public static Object[][] unmeasuredDurations() {
        return new Object[][]{
                {MetricsSupport.DEFAULT.operationDuration(null)},
                {MetricsSupport.DEFAULT.serializationDuration(null)},
                {MetricsSupport.DEFAULT.duration(null)},
        };
    }

    @Test(dataProvider = "unmeasuredDurations")
    public void testUnmeasuredDurationIsReportedAsUnknown(double duration) {
        Assert.assertEquals(duration, MetricsSupport.DURATION_UNKNOWN);
    }

    @Test
    public void testDurationIsConvertedToSeconds() {
        Assert.assertEquals(MetricsSupport.DEFAULT.duration(Duration.ofMillis(1500)), 1.5d);
        Assert.assertEquals(MetricName.OPERATION_DURATION.getUnit(), "s");
        Assert.assertEquals(MetricName.OPERATION_DURATION.getKey(), "db.client.operation.duration");
    }

    @Test
    public void testNullRecorderIsRejected() {
        Assert.assertThrows(NullPointerException.class, () -> new Client.Builder().setMetricsRecorder(null));
    }

    private Client.Builder newClientBuilder() {
        return new Client.Builder()
                .setUsername("default")
                .setPassword("")
                .setDefaultDatabase("test_db")
                .setMetricsRecorder(recorder);
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

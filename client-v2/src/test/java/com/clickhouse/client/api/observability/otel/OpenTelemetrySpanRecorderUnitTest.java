package com.clickhouse.client.api.observability.otel;

import com.clickhouse.client.api.ServerException;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.internal.ClientStatisticsHolder;
import com.clickhouse.client.api.metrics.OperationMetrics;
import com.clickhouse.client.api.metrics.OperationType;
import com.clickhouse.client.api.metrics.ServerMetrics;
import com.clickhouse.client.api.observability.DefaultSpanRecorder;
import com.clickhouse.client.api.observability.Span;
import com.clickhouse.client.api.observability.SpanAttribute;
import com.clickhouse.client.api.observability.SpanRecorder;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.client.api.transport.Endpoint;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.AttributeType;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.EventData;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;

public class OpenTelemetrySpanRecorderUnitTest {

    private static final String DATABASE = "spans_db";

    private InMemorySpanExporter exporter;
    private OpenTelemetrySdk openTelemetry;
    private OpenTelemetrySpanRecorder recorder;

    @BeforeMethod
    void setUp() {
        exporter = InMemorySpanExporter.create();
        openTelemetry = OpenTelemetrySdk.builder()
                .setTracerProvider(SdkTracerProvider.builder()
                        .addSpanProcessor(SimpleSpanProcessor.create(exporter))
                        .build())
                .build();
        recorder = new OpenTelemetrySpanRecorder(openTelemetry);
    }

    @AfterMethod
    void tearDown() {
        openTelemetry.close();
    }

    @Test
    public void testQuerySpanReportsStandardNameAndAttributes() {
        Span span = recorder.startQuerySpan(querySettings("q-42"), "SELECT 1", endpoint("ch-host", 8123));
        span.end();

        SpanData exported = onlySpan();
        Assert.assertEquals(exported.getName(), "query " + DATABASE);
        Assert.assertEquals(exported.getKind(), SpanKind.CLIENT);
        Assert.assertEquals(exported.getInstrumentationScopeInfo().getName(),
                OpenTelemetrySpanRecorder.INSTRUMENTATION_SCOPE_NAME);
        Assert.assertEquals(stringAttribute(exported, SpanAttribute.DB_SYSTEM_NAME), "clickhouse");
        Assert.assertEquals(stringAttribute(exported, SpanAttribute.DB_NAMESPACE), DATABASE);
        Assert.assertEquals(stringAttribute(exported, SpanAttribute.DB_QUERY_TEXT), "SELECT 1");
        Assert.assertEquals(stringAttribute(exported, SpanAttribute.CLICKHOUSE_QUERY_ID), "q-42");
        Assert.assertEquals(stringAttribute(exported, SpanAttribute.SERVER_ADDRESS), "ch-host");
        Assert.assertEquals(longAttribute(exported, SpanAttribute.SERVER_PORT), Long.valueOf(8123L));
        Assert.assertEquals(exported.getStatus().getStatusCode(), StatusCode.UNSET);
    }

    @Test
    public void testInsertSpanReportsCollectionAndBatchSize() {
        Span span = recorder.startInsertSpan(insertSettings("i-1"), "events", 5, endpoint("ch-host", 8123));
        span.end();

        SpanData exported = onlySpan();
        Assert.assertEquals(exported.getName(), "insert " + DATABASE + ".events");
        Assert.assertEquals(exported.getKind(), SpanKind.CLIENT);
        Assert.assertEquals(stringAttribute(exported, SpanAttribute.DB_OPERATION_NAME), "insert");
        Assert.assertEquals(stringAttribute(exported, SpanAttribute.DB_COLLECTION_NAME), "events");
        Assert.assertEquals(longAttribute(exported, SpanAttribute.DB_OPERATION_BATCH_SIZE), Long.valueOf(5L));
        Assert.assertNull(stringAttribute(exported, SpanAttribute.DB_QUERY_TEXT),
                "an insert sends no user statement");
    }

    @Test
    public void testInsertSpanOmitsUnknownBatchSize() {
        Span span = recorder.startInsertSpan(insertSettings("i-2"), "events", SpanRecorder.BATCH_SIZE_UNKNOWN,
                endpoint("ch-host", 8123));
        span.end();

        SpanData exported = onlySpan();
        Assert.assertEquals(exported.getName(), "insert " + DATABASE + ".events");
        Assert.assertNull(longAttribute(exported, SpanAttribute.DB_OPERATION_BATCH_SIZE));
    }

    @Test
    public void testRequestSpanIsChildOfOperationSpan() {
        Span operationSpan = recorder.startQuerySpan(querySettings("q-1"), "SELECT 1", endpoint("ch-host", 8123));
        Span requestSpan = recorder.startRequestSpan(operationSpan, "node-2", 8443);
        recorder.recordHttpStatus(requestSpan, 200);
        requestSpan.end();
        operationSpan.end();

        SpanData request = spanByName("POST");
        SpanData operation = spanByName("query " + DATABASE);
        Assert.assertEquals(request.getTraceId(), operation.getTraceId());
        Assert.assertEquals(request.getParentSpanId(), operation.getSpanId());
        Assert.assertEquals(request.getKind(), SpanKind.CLIENT);
        Assert.assertEquals(stringAttribute(request, SpanAttribute.HTTP_REQUEST_METHOD), "POST");
        Assert.assertEquals(longAttribute(request, SpanAttribute.HTTP_RESPONSE_STATUS_CODE), Long.valueOf(200L));
        Assert.assertEquals(stringAttribute(request, SpanAttribute.SERVER_ADDRESS), "node-2",
                "the attempt reports the endpoint it used");
        Assert.assertEquals(longAttribute(request, SpanAttribute.SERVER_PORT), Long.valueOf(8443L));
    }

    @Test
    public void testOperationSpanJoinsAmbientTrace() {
        io.opentelemetry.api.trace.Span ambient = openTelemetry.getTracer("test").spanBuilder("application")
                .startSpan();
        Span operationSpan;
        try (Scope scope = ambient.makeCurrent()) {
            operationSpan = recorder.startQuerySpan(querySettings("q-1"), "SELECT 1", null);
        }
        operationSpan.end();
        ambient.end();

        SpanData operation = spanByName("query " + DATABASE);
        Assert.assertEquals(operation.getTraceId(), ambient.getSpanContext().getTraceId());
        Assert.assertEquals(operation.getParentSpanId(), ambient.getSpanContext().getSpanId());
    }

    @Test
    public void testRequestSpanFallsBackToCurrentContextWhenOperationSpanIsForeign() {
        Span requestSpan = recorder.startRequestSpan(DefaultSpanRecorder.NOOP_SPAN, "node-1", 8123);
        requestSpan.end();

        SpanData request = onlySpan();
        Assert.assertEquals(request.getName(), "POST");
        Assert.assertFalse(request.getParentSpanContext().isValid(),
                "without an operation span and without an ambient context there is no parent to attach to");

        io.opentelemetry.api.trace.Span ambient = openTelemetry.getTracer("test").spanBuilder("application")
                .startSpan();
        Span secondRequestSpan;
        try (Scope scope = ambient.makeCurrent()) {
            secondRequestSpan = recorder.startRequestSpan(DefaultSpanRecorder.NOOP_SPAN, "node-1", 8123);
        }
        secondRequestSpan.end();
        ambient.end();

        Assert.assertEquals(exporter.getFinishedSpanItems().get(1).getParentSpanId(),
                ambient.getSpanContext().getSpanId(),
                "with an ambient context the request span is started under it");
    }

    @Test
    public void testEveryAttemptReportsItsOwnRequestSpanUnderOneOperationSpan() {
        Span operationSpan = recorder.startQuerySpan(querySettings("q-1"), "SELECT 1", null);
        Span firstAttempt = recorder.startRequestSpan(operationSpan, "node-1", 8123);
        recorder.recordRequestFailure(firstAttempt, new IllegalStateException("first attempt failed"));
        firstAttempt.end();
        Span secondAttempt = recorder.startRequestSpan(operationSpan, "node-2", 8123);
        recorder.recordHttpStatus(secondAttempt, 200);
        secondAttempt.end();
        operationSpan.end();

        List<SpanData> spans = exporter.getFinishedSpanItems();
        Assert.assertEquals(spans.size(), 3, "Unexpected spans: " + spans);
        SpanData operation = spans.get(2);
        Assert.assertEquals(spans.get(0).getParentSpanId(), operation.getSpanId());
        Assert.assertEquals(spans.get(1).getParentSpanId(), operation.getSpanId());
        Assert.assertEquals(spans.get(0).getStatus().getStatusCode(), StatusCode.ERROR);
        Assert.assertEquals(stringAttribute(spans.get(0), SpanAttribute.SERVER_ADDRESS), "node-1");
        Assert.assertEquals(spans.get(1).getStatus().getStatusCode(), StatusCode.UNSET);
        Assert.assertEquals(stringAttribute(spans.get(1), SpanAttribute.SERVER_ADDRESS), "node-2");
        Assert.assertEquals(operation.getStatus().getStatusCode(), StatusCode.UNSET,
                "a retried operation that succeeded is not failed");
    }

    @Test
    public void testQuerySuccessRecordsQueryIdAndWhatTheServerReadAndReturned() {
        OperationMetrics metrics = queryMetrics();
        metrics.setQueryId("server-assigned-id");
        metrics.updateMetric(ServerMetrics.RESULT_ROWS, 7);
        metrics.updateMetric(ServerMetrics.NUM_ROWS_READ, 4096);
        metrics.updateMetric(ServerMetrics.NUM_BYTES_READ, 65536);

        Span span = recorder.startQuerySpan(querySettings(null), "SELECT 1", null);
        recorder.recordQuerySuccess(span, metrics);
        span.end();

        SpanData exported = onlySpan();
        Assert.assertEquals(stringAttribute(exported, SpanAttribute.CLICKHOUSE_QUERY_ID), "server-assigned-id");
        Assert.assertEquals(longAttribute(exported, SpanAttribute.DB_RESPONSE_RETURNED_ROWS), Long.valueOf(7L));
        Assert.assertEquals(longAttribute(exported, SpanAttribute.CLICKHOUSE_RESPONSE_READ_ROWS), Long.valueOf(4096L));
        Assert.assertEquals(longAttribute(exported, SpanAttribute.CLICKHOUSE_RESPONSE_READ_BYTES), Long.valueOf(65536L));
        Assert.assertEquals(exported.getStatus().getStatusCode(), StatusCode.UNSET);
    }

    @Test
    public void testInsertSuccessRecordsQueryIdAndWhatTheServerWrote() {
        OperationMetrics metrics = insertMetrics();
        metrics.setQueryId("server-assigned-id");
        metrics.updateMetric(ServerMetrics.NUM_ROWS_WRITTEN, 12);
        metrics.updateMetric(ServerMetrics.NUM_BYTES_WRITTEN, 480);

        Span span = recorder.startInsertSpan(insertSettings(null), "t1", 12, null);
        recorder.recordInsertSuccess(span, metrics);
        span.end();

        SpanData exported = onlySpan();
        Assert.assertEquals(stringAttribute(exported, SpanAttribute.CLICKHOUSE_QUERY_ID), "server-assigned-id");
        Assert.assertEquals(longAttribute(exported, SpanAttribute.CLICKHOUSE_RESPONSE_WRITTEN_ROWS), Long.valueOf(12L));
        Assert.assertEquals(longAttribute(exported, SpanAttribute.CLICKHOUSE_RESPONSE_WRITTEN_BYTES), Long.valueOf(480L));
        Assert.assertEquals(exported.getStatus().getStatusCode(), StatusCode.UNSET);
    }

    @Test
    public void testEachTrackRecordsOnlyTheMetricsOfItsOwnOperation() {
        // the server reports the whole summary for both kinds of operation; each track picks the
        // metrics that are meaningful for it, so a query never claims written rows and the reverse
        OperationMetrics metrics = queryMetrics();
        metrics.updateMetric(ServerMetrics.RESULT_ROWS, 7);
        metrics.updateMetric(ServerMetrics.NUM_ROWS_READ, 4096);
        metrics.updateMetric(ServerMetrics.NUM_BYTES_READ, 65536);
        metrics.updateMetric(ServerMetrics.NUM_ROWS_WRITTEN, 12);
        metrics.updateMetric(ServerMetrics.NUM_BYTES_WRITTEN, 480);

        Span querySpan = recorder.startQuerySpan(querySettings(null), "SELECT 1", null);
        recorder.recordQuerySuccess(querySpan, metrics);
        querySpan.end();
        Span insertSpan = recorder.startInsertSpan(insertSettings(null), "t1", 12, null);
        recorder.recordInsertSuccess(insertSpan, metrics);
        insertSpan.end();

        List<SpanData> spans = exporter.getFinishedSpanItems();
        Assert.assertEquals(spans.size(), 2, "Unexpected spans: " + spans);
        SpanData query = spans.get(0);
        SpanData insert = spans.get(1);

        Assert.assertEquals(longAttribute(query, SpanAttribute.DB_RESPONSE_RETURNED_ROWS), Long.valueOf(7L));
        Assert.assertEquals(longAttribute(query, SpanAttribute.CLICKHOUSE_RESPONSE_READ_ROWS), Long.valueOf(4096L));
        Assert.assertNull(longAttribute(query, SpanAttribute.CLICKHOUSE_RESPONSE_WRITTEN_ROWS));
        Assert.assertNull(longAttribute(query, SpanAttribute.CLICKHOUSE_RESPONSE_WRITTEN_BYTES));

        Assert.assertEquals(longAttribute(insert, SpanAttribute.CLICKHOUSE_RESPONSE_WRITTEN_ROWS), Long.valueOf(12L));
        Assert.assertEquals(longAttribute(insert, SpanAttribute.CLICKHOUSE_RESPONSE_WRITTEN_BYTES), Long.valueOf(480L));
        Assert.assertNull(longAttribute(insert, SpanAttribute.DB_RESPONSE_RETURNED_ROWS));
        Assert.assertNull(longAttribute(insert, SpanAttribute.CLICKHOUSE_RESPONSE_READ_ROWS));
        Assert.assertNull(longAttribute(insert, SpanAttribute.CLICKHOUSE_RESPONSE_READ_BYTES));
    }

    @Test
    public void testMetricTheServerDidNotReportIsNotRecorded() {
        // ProcessParser sets every server metric to -1 before it applies the summary, so a metric
        // missing from the summary must not reach the span as a negative count
        OperationMetrics metrics = queryMetrics();
        metrics.updateMetric(ServerMetrics.RESULT_ROWS, -1);
        metrics.updateMetric(ServerMetrics.NUM_ROWS_READ, -1);
        metrics.updateMetric(ServerMetrics.NUM_BYTES_READ, 65536);

        Span span = recorder.startQuerySpan(querySettings(null), "SELECT 1", null);
        recorder.recordQuerySuccess(span, metrics);
        span.end();

        SpanData exported = onlySpan();
        Assert.assertNull(longAttribute(exported, SpanAttribute.DB_RESPONSE_RETURNED_ROWS));
        Assert.assertNull(longAttribute(exported, SpanAttribute.CLICKHOUSE_RESPONSE_READ_ROWS));
        Assert.assertEquals(longAttribute(exported, SpanAttribute.CLICKHOUSE_RESPONSE_READ_BYTES), Long.valueOf(65536L));
    }

    @Test
    public void testSuccessWithoutMetricsRecordsNothing() {
        Span querySpan = recorder.startQuerySpan(querySettings(null), "SELECT 1", null);
        recorder.recordQuerySuccess(querySpan, null);
        querySpan.end();
        Span insertSpan = recorder.startInsertSpan(insertSettings(null), "t1", 1, null);
        recorder.recordInsertSuccess(insertSpan, null);
        insertSpan.end();

        List<SpanData> spans = exporter.getFinishedSpanItems();
        Assert.assertEquals(spans.size(), 2, "Unexpected spans: " + spans);
        for (SpanData exported : spans) {
            Assert.assertNull(stringAttribute(exported, SpanAttribute.CLICKHOUSE_QUERY_ID));
            Assert.assertNull(longAttribute(exported, SpanAttribute.DB_RESPONSE_RETURNED_ROWS));
            Assert.assertNull(longAttribute(exported, SpanAttribute.CLICKHOUSE_RESPONSE_WRITTEN_ROWS));
            Assert.assertEquals(exported.getStatus().getStatusCode(), StatusCode.UNSET);
        }
    }

    @Test
    public void testFailureIsRecordedAsExceptionEvent() {
        Span span = recorder.startQuerySpan(querySettings("q-1"), "SELECT 1", null);
        recorder.recordFailure(span, new IllegalStateException("boom"));
        span.end();

        SpanData exported = onlySpan();
        Assert.assertEquals(exported.getEvents().size(), 1, "Unexpected events: " + exported.getEvents());
        EventData event = exported.getEvents().get(0);
        Assert.assertEquals(event.getAttributes().get(AttributeKey.stringKey("exception.type")),
                IllegalStateException.class.getName());
        Assert.assertEquals(event.getAttributes().get(AttributeKey.stringKey("exception.message")), "boom");
    }

    @Test
    public void testSpansAreReportedUnderTheGivenTracerScope() {
        OpenTelemetrySpanRecorder tracerRecorder =
                new OpenTelemetrySpanRecorder(openTelemetry.getTracer("application-scope", "1.2.3"));

        tracerRecorder.startQuerySpan(querySettings("q-1"), "SELECT 1", null).end();

        SpanData exported = onlySpan();
        Assert.assertEquals(exported.getInstrumentationScopeInfo().getName(), "application-scope");
        Assert.assertEquals(exported.getInstrumentationScopeInfo().getVersion(), "1.2.3");
        Assert.assertEquals(exported.getName(), "query " + DATABASE);
    }

    @Test
    public void testGlobalInstanceIsReadWhenSpanStartsNotWhenRecorderIsCreated() {
        GlobalOpenTelemetry.resetForTest();
        try {
            // the recorder is created before the application installs its SDK
            OpenTelemetrySpanRecorder globalRecorder = new OpenTelemetrySpanRecorder();

            InMemorySpanExporter lateExporter = InMemorySpanExporter.create();
            OpenTelemetrySdk lateSdk = OpenTelemetrySdk.builder()
                    .setTracerProvider(SdkTracerProvider.builder()
                            .addSpanProcessor(SimpleSpanProcessor.create(lateExporter))
                            .build())
                    .build();
            GlobalOpenTelemetry.set(lateSdk);
            try {
                globalRecorder.startQuerySpan(querySettings("q-late"), "SELECT 1", endpoint("ch-host", 8123)).end();

                List<SpanData> exported = lateExporter.getFinishedSpanItems();
                Assert.assertEquals(exported.size(), 1,
                        "a span must reach the SDK installed after the recorder was created");
                Assert.assertEquals(exported.get(0).getName(), "query " + DATABASE);
                Assert.assertEquals(exported.get(0).getInstrumentationScopeInfo().getName(),
                        OpenTelemetrySpanRecorder.INSTRUMENTATION_SCOPE_NAME);
            } finally {
                lateSdk.close();
            }
        } finally {
            GlobalOpenTelemetry.resetForTest();
        }
    }

    @Test
    public void testFailureRecordsErrorStatusAndErrorType() {
        Span span = recorder.startQuerySpan(querySettings("q-1"), "SELECT 1", null);
        recorder.recordFailure(span, new IllegalStateException("boom"));
        span.end();

        SpanData exported = onlySpan();
        Assert.assertEquals(exported.getStatus().getStatusCode(), StatusCode.ERROR);
        Assert.assertEquals(stringAttribute(exported, SpanAttribute.ERROR_TYPE),
                IllegalStateException.class.getName());
        Assert.assertNull(longAttribute(exported, SpanAttribute.DB_RESPONSE_STATUS_CODE),
                "a client-side failure carries no server error code");
    }

    @Test
    public void testServerFailureRecordsClickHouseCodeAndHttpStatus() {
        Span operationSpan = recorder.startQuerySpan(querySettings("q-1"), "SELECT 1", null);
        Span requestSpan = recorder.startRequestSpan(operationSpan, "node-1", 8123);
        ServerException serverException = new ServerException(60, "table not found", 404, "q-1");
        recorder.recordRequestFailure(requestSpan, serverException);
        recorder.recordFailure(operationSpan, new RuntimeException(serverException));
        requestSpan.end();
        operationSpan.end();

        SpanData request = spanByName("POST");
        Assert.assertEquals(request.getStatus().getStatusCode(), StatusCode.ERROR);
        Assert.assertEquals(stringAttribute(request, SpanAttribute.ERROR_TYPE), ServerException.class.getName());
        Assert.assertEquals(longAttribute(request, SpanAttribute.DB_RESPONSE_STATUS_CODE), Long.valueOf(60L));
        Assert.assertEquals(longAttribute(request, SpanAttribute.HTTP_RESPONSE_STATUS_CODE), Long.valueOf(404L));

        SpanData operation = spanByName("query " + DATABASE);
        Assert.assertEquals(operation.getStatus().getStatusCode(), StatusCode.ERROR);
        Assert.assertEquals(stringAttribute(operation, SpanAttribute.ERROR_TYPE), ServerException.class.getName());
        Assert.assertEquals(longAttribute(operation, SpanAttribute.DB_RESPONSE_STATUS_CODE), Long.valueOf(60L));
    }

    @Test
    public void testEndIsIdempotent() {
        Span span = recorder.startQuerySpan(querySettings("q-1"), "SELECT 1", null);
        span.end();
        span.end();

        Assert.assertEquals(exporter.getFinishedSpanItems().size(), 1);
    }

    @DataProvider(name = "attributeValues")
    public static Object[][] attributeValues() {
        return new Object[][]{
                {"text", AttributeType.STRING, "text"},
                {Boolean.TRUE, AttributeType.BOOLEAN, Boolean.TRUE},
                {42, AttributeType.LONG, 42L},
                {42L, AttributeType.LONG, 42L},
                {(short) 42, AttributeType.LONG, 42L},
                {1.5d, AttributeType.DOUBLE, 1.5d},
                {1.5f, AttributeType.DOUBLE, 1.5d},
                {URI.create("http://localhost:8123"), AttributeType.STRING, "http://localhost:8123"},
        };
    }

    @Test(dataProvider = "attributeValues")
    public void testAttributeValueTyping(Object value, AttributeType expectedType, Object expectedValue) {
        Span span = recorder.startQuerySpan(querySettings("q-1"), "SELECT 1", null);
        span.setAttribute("custom.attribute", value);
        span.end();

        SpanData exported = onlySpan();
        AttributeKey<?> key = keyOf(exported, "custom.attribute");
        Assert.assertNotNull(key, "attribute was not recorded");
        Assert.assertEquals(key.getType(), expectedType);
        Assert.assertEquals(exported.getAttributes().get(key), expectedValue);
    }

    @Test
    public void testNullAttributeKeyOrValueIsIgnored() {
        Span span = recorder.startQuerySpan(querySettings("q-1"), "SELECT 1", null);
        span.setAttribute(null, "value");
        span.setAttribute("custom.attribute", null);
        span.end();

        SpanData exported = onlySpan();
        Assert.assertNull(keyOf(exported, "custom.attribute"));
        Assert.assertEquals(stringAttribute(exported, SpanAttribute.DB_QUERY_TEXT), "SELECT 1",
                "the other attributes are still recorded");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNullOpenTelemetryIsRejected() {
        new OpenTelemetrySpanRecorder((OpenTelemetry) null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNullTracerIsRejected() {
        new OpenTelemetrySpanRecorder((Tracer) null);
    }

    @Test
    public void testMetricsReportTheKindTheyWereCreatedWith() {
        Assert.assertEquals(queryMetrics().getOperationType(), OperationType.QUERY);
        Assert.assertEquals(insertMetrics().getOperationType(), OperationType.INSERT);
    }

    @Test(expectedExceptions = NullPointerException.class,
            expectedExceptionsMessageRegExp = "operationType must not be null")
    public void testMetricsWithoutAKindAreRejected() {
        // the client always knows the kind of the operation it runs
        new OperationMetrics(new ClientStatisticsHolder(), null);
    }

    private OperationMetrics queryMetrics() {
        return new OperationMetrics(new ClientStatisticsHolder(), OperationType.QUERY);
    }

    private OperationMetrics insertMetrics() {
        return new OperationMetrics(new ClientStatisticsHolder(), OperationType.INSERT);
    }

    private QuerySettings querySettings(String queryId) {
        return new QuerySettings().setDatabase(DATABASE).setQueryId(queryId);
    }

    private InsertSettings insertSettings(String queryId) {
        return new InsertSettings().setDatabase(DATABASE).setQueryId(queryId);
    }

    private static Endpoint endpoint(String host, int port) {
        return new Endpoint() {
            @Override
            public URI getURI() {
                return URI.create("http://" + host + ":" + port);
            }

            @Override
            public String getHost() {
                return host;
            }

            @Override
            public int getPort() {
                return port;
            }
        };
    }

    private SpanData onlySpan() {
        List<SpanData> spans = exporter.getFinishedSpanItems();
        Assert.assertEquals(spans.size(), 1, "Unexpected spans: " + spans);
        return spans.get(0);
    }

    private SpanData spanByName(String name) {
        for (SpanData span : exporter.getFinishedSpanItems()) {
            if (name.equals(span.getName())) {
                return span;
            }
        }
        Assert.fail("No span named '" + name + "' in " + exporter.getFinishedSpanItems());
        return null;
    }

    private static String stringAttribute(SpanData span, SpanAttribute attribute) {
        return span.getAttributes().get(AttributeKey.stringKey(attribute.getKey()));
    }

    private static Long longAttribute(SpanData span, SpanAttribute attribute) {
        return span.getAttributes().get(AttributeKey.longKey(attribute.getKey()));
    }

    private static AttributeKey<?> keyOf(SpanData span, String key) {
        for (AttributeKey<?> candidate : span.getAttributes().asMap().keySet()) {
            if (candidate.getKey().equals(key)) {
                return candidate;
            }
        }
        return null;
    }
}

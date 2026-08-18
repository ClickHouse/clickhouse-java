package com.clickhouse.client.observability;

import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.observability.CapturingSpanRecorder;
import com.clickhouse.client.api.observability.CapturingSpanRecorder.CapturedSpan;
import com.clickhouse.client.api.observability.DefaultSpanRecorder;
import com.clickhouse.client.api.observability.Span;
import com.clickhouse.client.api.observability.SpanAttribute;
import com.clickhouse.client.api.observability.SpanRecorder;
import com.clickhouse.client.api.observability.SpanSupport;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.client.api.transport.Endpoint;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Asserts the layering of the observability SPI as seen from outside its own package - the way a
 * recorder shipped separately uses it: the recorder owns the spans and calls {@link SpanSupport} when
 * it wants the client's standard names and attributes, and it is free not to call it at all.
 */
public class SpanSupportUsageTest {

    @Test
    public void testSupportFillsStandardValuesOnARecordersOwnSpan() {
        // the shape of a recorder implementation: it creates the span, then asks the support to fill
        // the standard name and attributes from the structures the client handed it
        SpanSupport support = SpanSupport.DEFAULT;
        QuerySettings settings = new QuerySettings().setDatabase("db1").setQueryId("q1");
        RecordingSpan span = new RecordingSpan();

        Assert.assertEquals(support.querySpanName(settings), "query db1");
        support.fillQueryAttributes(span, settings, "SELECT 1", null);
        support.recordFailure(span, new IllegalStateException("boom"));

        Assert.assertEquals(span.attributes.get(SpanAttribute.DB_SYSTEM_NAME.getKey()), "clickhouse");
        Assert.assertEquals(span.attributes.get(SpanAttribute.DB_NAMESPACE.getKey()), "db1");
        Assert.assertEquals(span.attributes.get(SpanAttribute.DB_QUERY_TEXT.getKey()), "SELECT 1");
        Assert.assertEquals(span.attributes.get(SpanAttribute.CLICKHOUSE_QUERY_ID.getKey()), "q1");
        Assert.assertEquals(span.errorType, IllegalStateException.class.getName());
    }

    @Test
    public void testSupportFillsInsertAndRequestValues() {
        SpanSupport support = SpanSupport.DEFAULT;
        InsertSettings settings = new InsertSettings().setDatabase("db1").setQueryId("q1");
        RecordingSpan operationSpan = new RecordingSpan();
        RecordingSpan requestSpan = new RecordingSpan();

        Assert.assertEquals(support.insertSpanName(settings, "t1"), "insert db1.t1");
        support.fillInsertAttributes(operationSpan, settings, "t1", 3, null);
        Assert.assertEquals(support.requestSpanName(), "POST");
        support.fillRequestAttributes(requestSpan, "localhost", 8123);
        support.recordHttpStatus(requestSpan, 200);

        Assert.assertEquals(operationSpan.attributes.get(SpanAttribute.DB_COLLECTION_NAME.getKey()), "t1");
        Assert.assertEquals(operationSpan.attributes.get(SpanAttribute.DB_OPERATION_NAME.getKey()), "insert");
        Assert.assertEquals(operationSpan.attributes.get(SpanAttribute.DB_OPERATION_BATCH_SIZE.getKey()), 3);
        Assert.assertEquals(requestSpan.attributes.get(SpanAttribute.HTTP_REQUEST_METHOD.getKey()), "POST");
        Assert.assertEquals(requestSpan.attributes.get(SpanAttribute.SERVER_ADDRESS.getKey()), "localhost");
        Assert.assertEquals(requestSpan.attributes.get(SpanAttribute.SERVER_PORT.getKey()), 8123);
        Assert.assertEquals(requestSpan.attributes.get(SpanAttribute.HTTP_RESPONSE_STATUS_CODE.getKey()), 200);
    }

    @Test
    public void testUnknownBatchSizeIsNotReported() {
        SpanSupport support = SpanSupport.DEFAULT;
        RecordingSpan span = new RecordingSpan();

        support.fillInsertAttributes(span, new InsertSettings().setDatabase("db1"), "t1",
                SpanRecorder.BATCH_SIZE_UNKNOWN, null);

        Assert.assertFalse(span.attributes.containsKey(SpanAttribute.DB_OPERATION_BATCH_SIZE.getKey()),
                "a batch size the client does not know must not be reported");
    }

    @Test
    public void testSupportMayBeExtendedToChangeSpanNames() {
        // a recorder that wants other values overrides the method that computes them and hands its own
        // support to the client's values
        SpanSupport support = new SpanSupport() {
            @Override
            protected String spanName(String operationName, String namespace, String collectionName) {
                return "custom:" + super.spanName(operationName, namespace, collectionName);
            }
        };

        Assert.assertEquals(support.querySpanName(new QuerySettings().setDatabase("db1")), "custom:query db1");
    }

    @Test
    public void testRecorderIsCalledFirstAndMayIgnoreTheSupport() {
        // the client calls the recorder, not the support, so a recorder that reports its own values
        // never goes through SpanSupport at all
        OwnValuesSpanRecorder recorder = new OwnValuesSpanRecorder();

        Span span = recorder.startQuerySpan(new QuerySettings().setDatabase("db1"), "SELECT 1", null);
        span.end();

        Assert.assertEquals(((RecordingSpan) span).attributes.size(), 1,
                "a recorder that ignores the support reports only what it set itself");
        Assert.assertEquals(((RecordingSpan) span).attributes.get("my.statement"), "SELECT 1");
        Assert.assertFalse(recorder.usedSupport, "using SpanSupport must stay opt-in");
    }

    @Test
    public void testRecorderThatOptsInGetsTheStandardValues() {
        CapturingSpanRecorder recorder = new CapturingSpanRecorder();

        Span span = recorder.startQuerySpan(new QuerySettings().setDatabase("db1").setQueryId("q1"),
                "SELECT 1", null);
        span.end();

        CapturedSpan captured = recorder.operationSpan();
        Assert.assertSame(captured, span);
        Assert.assertEquals(captured.getName(), "query db1");
        Assert.assertEquals(captured.getAttribute(SpanAttribute.DB_SYSTEM_NAME), "clickhouse");
        Assert.assertEquals(captured.getAttribute(SpanAttribute.DB_QUERY_TEXT), "SELECT 1");
        Assert.assertEquals(captured.getAttribute(SpanAttribute.CLICKHOUSE_QUERY_ID), "q1");
        Assert.assertEquals(captured.getEndCount(), 1);
    }

    @Test
    public void testDefaultRecorderRecordsNothing() {
        Assert.assertSame(DefaultSpanRecorder.NOOP.startQuerySpan(new QuerySettings().setDatabase("db1"),
                "SELECT 1", null), DefaultSpanRecorder.NOOP_SPAN);
        Assert.assertSame(DefaultSpanRecorder.NOOP.startRequestSpan(DefaultSpanRecorder.NOOP_SPAN,
                "localhost", 8123), DefaultSpanRecorder.NOOP_SPAN);
    }

    /**
     * Recorder that reports its own values and never asks {@link SpanSupport} for the standard ones.
     */
    private static final class OwnValuesSpanRecorder extends DefaultSpanRecorder {

        volatile boolean usedSupport;

        @Override
        protected SpanSupport getSpanSupport() {
            usedSupport = true;
            return super.getSpanSupport();
        }

        @Override
        public Span startQuerySpan(QuerySettings settings, String sqlQuery, Endpoint endpoint) {
            RecordingSpan span = new RecordingSpan();
            span.setAttribute("my.statement", sqlQuery);
            return span;
        }
    }

    /**
     * Span outside the client's packages, so the test uses the SPI the way an external recorder does.
     */
    private static final class RecordingSpan implements Span {

        final Map<String, Object> attributes = new LinkedHashMap<>();
        String errorType;

        @Override
        public void setAttribute(String key, Object value) {
            attributes.put(key, value);
        }

        @Override
        public void setError(String errorType) {
            this.errorType = errorType;
        }

        @Override
        public void end() {
            // nothing to do
        }
    }
}

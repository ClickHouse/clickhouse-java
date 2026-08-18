package com.clickhouse.client.observability;

import com.clickhouse.client.api.observability.CapturingSpanRecorder;
import com.clickhouse.client.api.observability.CapturingSpanRecorder.CapturedSpan;
import com.clickhouse.client.api.observability.DefaultSpanRecorder;
import com.clickhouse.client.api.observability.Span;
import com.clickhouse.client.api.observability.SpanAttribute;
import com.clickhouse.client.api.observability.SpanSupport;
import com.clickhouse.client.api.query.QuerySettings;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Asserts that {@link SpanSupport} is usable and extensible from outside its own package, so a
 * recorder implementation shipped separately can reuse the client's attribute values.
 */
public class SpanSupportUsageTest {

    @Test
    public void testSupportIsUsableFromAnotherPackage() {
        CapturingSpanRecorder recorder = new CapturingSpanRecorder();
        SpanSupport spanSupport = new SpanSupport(recorder);
        Assert.assertTrue(spanSupport.isEnabled());

        Span span = spanSupport.startQuerySpan(new QuerySettings().setDatabase("db1").setQueryId("q1"),
                "SELECT 1", null);
        spanSupport.recordFailure(span, new IllegalStateException("boom"));
        span.end();

        CapturedSpan captured = recorder.operationSpan();
        Assert.assertSame(captured, span);
        Assert.assertEquals(captured.getName(), "query db1");
        Assert.assertEquals(captured.getAttribute(SpanAttribute.DB_SYSTEM_NAME), "clickhouse");
        Assert.assertEquals(captured.getAttribute(SpanAttribute.DB_NAMESPACE), "db1");
        Assert.assertEquals(captured.getAttribute(SpanAttribute.DB_QUERY_TEXT), "SELECT 1");
        Assert.assertEquals(captured.getAttribute(SpanAttribute.CLICKHOUSE_QUERY_ID), "q1");
        Assert.assertEquals(captured.getErrorType(), IllegalStateException.class.getName());
        Assert.assertEquals(captured.getEndCount(), 1);
    }

    @Test
    public void testSupportMayBeExtendedToChangeSpanNames() {
        CapturingSpanRecorder recorder = new CapturingSpanRecorder();
        SpanSupport spanSupport = new SpanSupport(recorder) {
            @Override
            protected String spanName(String operationName, String namespace, String collectionName) {
                return "custom:" + super.spanName(operationName, namespace, collectionName);
            }
        };

        spanSupport.startQuerySpan(new QuerySettings().setDatabase("db1"), "SELECT 1", null).end();

        Assert.assertEquals(recorder.operationSpan().getName(), "custom:query db1");
    }

    @Test
    public void testDisabledSupportRecordsNothing() {
        CapturingSpanRecorder recorder = new CapturingSpanRecorder();

        Assert.assertFalse(SpanSupport.DISABLED.isEnabled());
        Assert.assertFalse(new SpanSupport(DefaultSpanRecorder.NOOP).isEnabled(),
                "a recorder that records nothing keeps the fast path");

        Span span = SpanSupport.DISABLED.startQuerySpan(new QuerySettings().setDatabase("db1"),
                "SELECT 1", null);
        Assert.assertSame(span, DefaultSpanRecorder.NOOP_SPAN);
        Assert.assertSame(SpanSupport.DISABLED.startRequestSpan(span, "localhost", 8123),
                DefaultSpanRecorder.NOOP_SPAN);
        Assert.assertTrue(recorder.getSpans().isEmpty());
    }

    @Test
    public void testRecorderIsRequired() {
        // the client's default recorder already records nothing, so a null recorder is a
        // configuration error rather than a way to disable recording
        Assert.assertThrows(NullPointerException.class, () -> new SpanSupport(null));
    }
}

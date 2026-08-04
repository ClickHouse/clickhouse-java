package com.clickhouse.client.api.observability;

import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.query.QuerySettings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Recorder that keeps every span it starts, so tests can assert what the client reported.
 */
public class CapturingSpanRecorder extends DefaultSpanRecorder {

    private final List<CapturedSpan> spans = Collections.synchronizedList(new ArrayList<>());

    @Override
    public Span startSpan(String spanName, QuerySettings settings) {
        return add(new CapturedSpan(spanName, null, settings.getDatabase(), settings.getQueryId()));
    }

    @Override
    public Span startSpan(String spanName, InsertSettings settings) {
        return add(new CapturedSpan(spanName, null, settings.getDatabase(), settings.getQueryId()));
    }

    @Override
    public Span startRequestSpan(String spanName, Span operationSpan) {
        return add(new CapturedSpan(spanName, operationSpan, null, null));
    }

    private CapturedSpan add(CapturedSpan span) {
        spans.add(span);
        return span;
    }

    public List<CapturedSpan> getSpans() {
        synchronized (spans) {
            return new ArrayList<>(spans);
        }
    }

    /**
     * Returns the only operation span recorded so far.
     */
    public CapturedSpan operationSpan() {
        List<CapturedSpan> operations = new ArrayList<>();
        for (CapturedSpan span : getSpans()) {
            if (span.getParent() == null) {
                operations.add(span);
            }
        }
        if (operations.size() != 1) {
            throw new AssertionError("Expected exactly one operation span but got " + operations);
        }
        return operations.get(0);
    }

    /**
     * Returns the request spans started for the given operation span, in the order they were started.
     */
    public List<CapturedSpan> requestSpans(CapturedSpan operationSpan) {
        List<CapturedSpan> requests = new ArrayList<>();
        for (CapturedSpan span : getSpans()) {
            if (span.getParent() == operationSpan) {
                requests.add(span);
            }
        }
        return requests;
    }

    public void clear() {
        spans.clear();
    }

    public static final class CapturedSpan implements Span {

        private final String name;
        private final Span parent;
        private final String settingsDatabase;
        private final String settingsQueryId;
        private final Map<String, Object> attributes = Collections.synchronizedMap(new LinkedHashMap<>());
        private final AtomicInteger endCount = new AtomicInteger();
        private volatile String errorType;

        CapturedSpan(String name, Span parent, String settingsDatabase, String settingsQueryId) {
            this.name = name;
            this.parent = parent;
            this.settingsDatabase = settingsDatabase;
            this.settingsQueryId = settingsQueryId;
        }

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
            endCount.incrementAndGet();
        }

        public String getName() {
            return name;
        }

        public Span getParent() {
            return parent;
        }

        public String getSettingsDatabase() {
            return settingsDatabase;
        }

        public String getSettingsQueryId() {
            return settingsQueryId;
        }

        public Map<String, Object> getAttributes() {
            synchronized (attributes) {
                return new LinkedHashMap<>(attributes);
            }
        }

        public Object getAttribute(SpanAttribute attribute) {
            return attributes.get(attribute.getKey());
        }

        public Object getAttribute(String key) {
            return attributes.get(key);
        }

        public String getErrorType() {
            return errorType;
        }

        public int getEndCount() {
            return endCount.get();
        }

        @Override
        public String toString() {
            return "CapturedSpan{name='" + name + "', attributes=" + getAttributes()
                    + ", errorType=" + errorType + ", endCount=" + endCount.get() + '}';
        }
    }
}

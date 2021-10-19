package com.clickhouse.client.data;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.clickhouse.client.ClickHouseColumn;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseDataProcessor;
import com.clickhouse.client.ClickHouseDataStreamFactory;
import com.clickhouse.client.ClickHouseFormat;
import com.clickhouse.client.ClickHouseRecord;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseResponseSummary;
import com.clickhouse.client.logging.Logger;
import com.clickhouse.client.logging.LoggerFactory;

/**
 * A stream response from server.
 */
public class ClickHouseStreamResponse implements ClickHouseResponse {
    private static final Logger log = LoggerFactory.getLogger(ClickHouseStreamResponse.class);

    private static final long serialVersionUID = 2271296998310082447L;

    private static final ClickHouseResponseSummary emptySummary = new ClickHouseResponseSummary() {
    };

    protected static final List<ClickHouseColumn> defaultTypes = Collections
            .singletonList(ClickHouseColumn.of("results", "Nullable(String)"));

    protected final ClickHouseConfig config;
    protected final transient InputStream input;
    protected final transient ClickHouseDataProcessor processor;
    protected final List<ClickHouseColumn> columns;

    private boolean isClosed;

    protected ClickHouseStreamResponse(ClickHouseConfig config, Map<String, Object> settings, InputStream input,
            List<ClickHouseColumn> columns) throws IOException {
        if (config == null || input == null) {
            throw new IllegalArgumentException("Non-null configuration and input stream are required");
        }

        this.config = config;
        this.input = input;

        boolean hasError = true;
        try {
            this.processor = ClickHouseDataStreamFactory.getInstance().getProcessor(config, input, null, settings,
                    columns);
            this.columns = columns != null ? columns
                    : (processor != null ? processor.getColumns() : Collections.emptyList());
            hasError = false;
        } finally {
            if (hasError) {
                // rude but safe
                log.error("Failed to create stream response, closing input stream");
                try {
                    input.close();
                } catch (Exception e) {
                    // ignore
                }
            }
        }

        this.isClosed = !hasError;
    }

    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public void close() {
        if (input != null) {
            long skipped = 0L;
            try {
                skipped = input.skip(Long.MAX_VALUE);
                log.debug("%d bytes skipped before closing input stream", skipped);
            } catch (Exception e) {
                // ignore
                log.debug("%d bytes skipped before closing input stream", skipped, e);
            } finally {
                try {
                    input.close();
                } catch (Exception e) {
                    log.warn("Failed to close input stream", e);
                }
                isClosed = true;
            }
        }
    }

    @Override
    public List<ClickHouseColumn> getColumns() {
        return columns;
    }

    public ClickHouseFormat getFormat() {
        return this.config.getFormat();
    }

    @Override
    public ClickHouseResponseSummary getSummary() {
        return emptySummary;
    }

    @Override
    public InputStream getInputStream() {
        return input;
    }

    @Override
    public Iterable<ClickHouseRecord> records() {
        if (processor == null) {
            throw new UnsupportedOperationException(
                    "No data processor available for deserialization, please consider to use getInputStream instead");
        }

        return processor.records();
    }
}

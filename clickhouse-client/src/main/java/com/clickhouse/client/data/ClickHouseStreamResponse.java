package com.clickhouse.client.data;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.clickhouse.client.ClickHouseColumn;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseDataProcessor;
import com.clickhouse.client.ClickHouseDataStreamFactory;
import com.clickhouse.client.ClickHouseFormat;
import com.clickhouse.client.ClickHouseInputStream;
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

    protected static final List<ClickHouseColumn> defaultTypes = Collections
            .singletonList(ClickHouseColumn.of("results", "Nullable(String)"));

    public static ClickHouseResponse of(ClickHouseConfig config, ClickHouseInputStream input) throws IOException {
        return of(config, input, null, null, null);
    }

    public static ClickHouseResponse of(ClickHouseConfig config, ClickHouseInputStream input,
            Map<String, Object> settings) throws IOException {
        return of(config, input, settings, null, null);
    }

    public static ClickHouseResponse of(ClickHouseConfig config, ClickHouseInputStream input,
            List<ClickHouseColumn> columns) throws IOException {
        return of(config, input, null, columns, null);
    }

    public static ClickHouseResponse of(ClickHouseConfig config, ClickHouseInputStream input,
            Map<String, Object> settings, List<ClickHouseColumn> columns) throws IOException {
        return of(config, input, settings, columns, null);
    }

    public static ClickHouseResponse of(ClickHouseConfig config, ClickHouseInputStream input,
            Map<String, Object> settings, List<ClickHouseColumn> columns, ClickHouseResponseSummary summary)
            throws IOException {
        return new ClickHouseStreamResponse(config, input, settings, columns, summary);
    }

    protected final ClickHouseConfig config;
    protected final transient ClickHouseInputStream input;
    protected final transient ClickHouseDataProcessor processor;
    protected final List<ClickHouseColumn> columns;
    protected final ClickHouseResponseSummary summary;

    private boolean closed;

    protected ClickHouseStreamResponse(ClickHouseConfig config, ClickHouseInputStream input,
            Map<String, Object> settings, List<ClickHouseColumn> columns, ClickHouseResponseSummary summary)
            throws IOException {
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
        this.summary = summary != null ? summary : ClickHouseResponseSummary.EMPTY;
        this.closed = hasError;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        if (closed || input.isClosed()) {
            return;
        }

        try {
            log.debug("%d bytes skipped before closing input stream", input.skip(Long.MAX_VALUE));
        } catch (Exception e) {
            // ignore
            log.debug("Failed to skip reading input stream due to: %s", e.getMessage());
        } finally {
            try {
                input.close();
            } catch (Exception e) {
                log.warn("Failed to close input stream", e);
            }
            closed = true;
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
        return summary;
    }

    @Override
    public ClickHouseInputStream getInputStream() {
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

    @Override
    public ClickHouseRecord totals() {
        return processor.totals();
    }

    @Override
    public ClickHouseRecord[] extremes() {
        return processor.extremes();
    }
}

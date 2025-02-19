package com.clickhouse.client;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.config.ClickHouseOption;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataProcessor;
import com.clickhouse.data.ClickHouseDataStreamFactory;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.logging.Logger;
import com.clickhouse.logging.LoggerFactory;

/**
 * A stream response from server.
 */
@Deprecated
public class ClickHouseStreamResponse implements ClickHouseResponse {
    private static final Logger log = LoggerFactory.getLogger(ClickHouseStreamResponse.class);

    private static final long serialVersionUID = 2271296998310082447L;

    private final TimeZone timeZone;

    protected static final List<ClickHouseColumn> defaultTypes = Collections
            .singletonList(ClickHouseColumn.of("results", "Nullable(String)"));
//
//    public static ClickHouseResponse of(ClickHouseConfig config, ClickHouseInputStream input) throws IOException {
//        return of(config, input, null, null, null);
//    }
//
//    public static ClickHouseResponse of(ClickHouseConfig config, ClickHouseInputStream input,
//            Map<String, Serializable> settings) throws IOException {
//        return of(config, input, settings, null, null);
//    }
//
//    public static ClickHouseResponse of(ClickHouseConfig config, ClickHouseInputStream input,
//            List<ClickHouseColumn> columns) throws IOException {
//        return of(config, input, null, columns, null);
//    }
//
//    public static ClickHouseResponse of(ClickHouseConfig config, ClickHouseInputStream input,
//            Map<String, Serializable> settings, List<ClickHouseColumn> columns) throws IOException {
//        return of(config, input, settings, columns, null);
//    }
//
    public static ClickHouseResponse of(ClickHouseConfig config, ClickHouseInputStream input,
                                        Map<String, Serializable> settings, List<ClickHouseColumn> columns,
                                        ClickHouseResponseSummary summary, TimeZone timeZone)
            throws IOException {
        return new ClickHouseStreamResponse(config, input, settings, columns, summary, timeZone);
    }

    protected final ClickHouseConfig config;
    protected final transient ClickHouseDataProcessor processor;
    protected final ClickHouseResponseSummary summary;

    private volatile boolean closed;

    protected ClickHouseStreamResponse(ClickHouseConfig config, ClickHouseInputStream input,
            Map<String, Serializable> settings, List<ClickHouseColumn> columns, ClickHouseResponseSummary summary,
                                       TimeZone timeZone)
            throws IOException {

        this.timeZone = timeZone;
        boolean hasError = true;
        try {
            if (timeZone != null && config.isUseServerTimeZone() && !config.getUseTimeZone().equals(timeZone)) {
                Map<ClickHouseOption, Serializable> configOptions = new HashMap<>(config.getAllOptions());
                configOptions.put(ClickHouseClientOption.SERVER_TIME_ZONE, timeZone.getID());
                config = new ClickHouseConfig(configOptions);
            }
            this.processor = ClickHouseDataStreamFactory.getInstance().getProcessor(config, input, null, settings,
                    columns);
            hasError = false;
        } finally {
            if (hasError) {
                // rude but safe
                log.error("Failed to create stream response, closing input stream");
                try {
                    input.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }

        this.config = config;

        this.closed = hasError;
        this.summary = summary != null ? summary : ClickHouseResponseSummary.EMPTY;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        final ClickHouseInputStream input = processor.getInputStream();
        if (closed || input.isClosed()) {
            return;
        }

        try {
            long skipped = input.skip(Long.MAX_VALUE);
            if (skipped > 0L) {
                log.debug("%d bytes skipped before closing input stream", skipped);
            }
        } catch (Exception e) {
            // ignore
            log.debug("Failed to skip reading input stream due to: %s", e.getMessage());
        } finally {
            // close forcibly without skipping won't help much when network is slow/unstable
            try {
                input.close();
            } catch (IOException e) {
                log.warn("Failed to close input stream", e);
            } finally {
                closed = true;
            }
        }
    }

    @Override
    public List<ClickHouseColumn> getColumns() {
        return this.processor.getColumns();
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
        return processor.getInputStream();
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
    public TimeZone getTimeZone() {
        return timeZone;
    }

    @Override
    public <T> Iterable<T> records(Class<T> objClass) {
        if (processor == null) {
            throw new UnsupportedOperationException(
                    "No data processor available for deserialization, please consider to use getInputStream instead");
        }
        return processor.records(objClass, null);
    }
}

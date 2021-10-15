package com.clickhouse.client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import com.clickhouse.client.exception.ClickHouseException;
import com.clickhouse.client.exception.ClickHouseExceptionSpecifier;
import com.clickhouse.client.logging.Logger;
import com.clickhouse.client.logging.LoggerFactory;

/**
 * This represents server reponse. To get data returned from server, depending
 * on actual needs, you have 3 options:
 *
 * <ul>
 * <li>use {@link #records()} or {@link #recordStream()} to get deserialized
 * records(usually rows), a record is composed of one or more values</li>
 * <li>use {@link #values()} or {@link #valueStream()} to get deserialized
 * values</li>
 * <li>use {@link #getInputStream()} for custom processing like dumping results
 * into a file</li>
 * </ul>
 */
public class ClickHouseResponse implements AutoCloseable, Serializable {
    private static final Logger log = LoggerFactory.getLogger(ClickHouseResponse.class);

    private static final long serialVersionUID = 2271296998310082447L;

    private static final ClickHouseResponseSummary emptySummary = new ClickHouseResponseSummary() {
    };

    protected static final List<ClickHouseColumn> defaultTypes = Collections
            .singletonList(ClickHouseColumn.of("results", "Nullable(String)"));

    protected final ClickHouseConfig config;
    protected final ClickHouseNode server;
    protected final InputStream input;
    protected final ClickHouseDataProcessor processor;
    protected final List<ClickHouseColumn> columns;
    protected final Throwable error;

    protected ClickHouseResponse(ClickHouseConfig config, ClickHouseNode server, Throwable error)
            throws ClickHouseException {
        this(config, server, null, null, null, error);
    }

    protected ClickHouseResponse(ClickHouseConfig config, ClickHouseNode server, Map<String, Object> settings,
            InputStream input, List<ClickHouseColumn> columns, Throwable error) throws ClickHouseException {
        try {
            this.config = ClickHouseChecker.nonNull(config, "config");
            this.server = ClickHouseChecker.nonNull(server, "server");

            if (error != null) {
                this.processor = null;
                this.input = null;
                // response object may be constructed in a separate thread
                this.error = error;
            } else if (input == null) {
                throw new IllegalArgumentException("input cannot be null when there's no error");
            } else {
                this.input = input;
                this.processor = ClickHouseDataStreamFactory.getInstance().getProcessor(config, input, null, settings,
                        columns);

                this.error = null;
            }

            this.columns = columns != null ? columns
                    : (processor != null ? processor.getColumns() : Collections.emptyList());
        } catch (IOException | RuntimeException e) { // TODO and Error?
            if (input != null) {
                log.warn("Failed to instantiate response object, will try to close the given input stream");
                try {
                    input.close();
                } catch (IOException exp) {
                    log.warn("Failed to close given input stream", exp);
                }
            }

            throw ClickHouseExceptionSpecifier.specify(e, server);
        }
    }

    protected void throwErrorIfAny() throws ClickHouseException {
        if (error == null) {
            return;
        }

        if (error instanceof ClickHouseException) {
            throw (ClickHouseException) error;
        } else {
            throw ClickHouseExceptionSpecifier.specify(error, server);
        }
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
            }
        }
    }

    public boolean hasError() {
        return error != null;
    }

    public List<ClickHouseColumn> getColumns() throws ClickHouseException {
        throwErrorIfAny();

        return columns;
    }

    public ClickHouseFormat getFormat() throws ClickHouseException {
        throwErrorIfAny();

        return this.config.getFormat();
    }

    public ClickHouseNode getServer() {
        return server;
    }

    public ClickHouseResponseSummary getSummary() {
        return emptySummary;
    }

    /**
     * This is the most memory-efficient way for you to handle data returned from
     * ClickHouse. However, this also means additional work is required for
     * deserialization, especially when using a binary format.
     *
     * @return input stream get raw data returned from server
     * @throws ClickHouseException when failed to get input stream or read data
     */
    public InputStream getInputStream() throws ClickHouseException {
        throwErrorIfAny();

        return input;
    }

    /**
     * Dump response into output stream.
     *
     * @param output output stream, which will remain open
     * @throws ClickHouseException when error occurred dumping response and/or
     *                             writing data into output stream
     */
    public void dump(OutputStream output) throws ClickHouseException {
        throwErrorIfAny();

        ClickHouseChecker.nonNull(output, "output");

        // TODO configurable buffer size
        int size = 8192;
        byte[] buffer = new byte[size];
        int counter = 0;
        try {
            while ((counter = input.read(buffer, 0, size)) >= 0) {
                output.write(buffer, 0, counter);
            }
        } catch (IOException e) {
            throw ClickHouseExceptionSpecifier.specify(e, server);
        }
    }

    public Iterable<ClickHouseRecord> records() throws ClickHouseException {
        throwErrorIfAny();

        if (processor == null) {
            throw new UnsupportedOperationException(
                    "No data processor available for deserialization, please use getInputStream instead");
        }

        return processor.records();
    }

    public Stream<ClickHouseRecord> recordStream() throws ClickHouseException {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(records().iterator(),
                Spliterator.IMMUTABLE | Spliterator.NONNULL | Spliterator.ORDERED), false);
    }

    public Iterable<ClickHouseValue> values() throws ClickHouseException {
        throwErrorIfAny();

        if (processor == null) {
            throw new UnsupportedOperationException(
                    "No data processor available for deserialization, please use getInputStream instead");
        }

        return new Iterable<ClickHouseValue>() {
            @Override
            public Iterator<ClickHouseValue> iterator() {
                return new Iterator<ClickHouseValue>() {
                    Iterator<ClickHouseRecord> records = processor.records().iterator();
                    ClickHouseRecord current = null;
                    int index = 0;

                    @Override
                    public boolean hasNext() {
                        return records.hasNext() || (current != null && index < current.size());
                    }

                    @Override
                    public ClickHouseValue next() {
                        if (current == null || index == current.size()) {
                            current = records.next();
                            index = 0;
                        }

                        try {
                            return current.getValue(index++);
                        } catch (IOException e) {
                            throw new IllegalStateException(e);
                        }
                    }
                };
            }
        };
    }

    public Stream<ClickHouseValue> valueStream() throws ClickHouseException {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(values().iterator(),
                Spliterator.IMMUTABLE | Spliterator.NONNULL | Spliterator.ORDERED), false);
    }
}

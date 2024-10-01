package com.clickhouse.client.api.query;

import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.data_formats.internal.BinaryReaderBackedRecord;
import com.clickhouse.client.api.metrics.OperationMetrics;
import com.clickhouse.client.api.metrics.ServerMetrics;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Records implements Iterable<GenericRecord>, AutoCloseable {

    private final QueryResponse response;

    private final ClickHouseBinaryFormatReader reader;

    private boolean empty;
    private Iterator<GenericRecord> iterator;

    public Records(QueryResponse response, ClickHouseBinaryFormatReader reader) {
        this.response = response;
        this.reader = reader;
        this.empty = !reader.hasNext();
    }

    @Override
    public Iterator<GenericRecord> iterator() {
        if (iterator == null) {
            iterator = new Iterator<GenericRecord>() {
                GenericRecord record = new BinaryReaderBackedRecord(reader);

                @Override
                public boolean hasNext() {
                    return reader.hasNext();
                }

                @Override
                public GenericRecord next() {
                    reader.next();
                    return record;
                }
            };
        } else {
            throw new IllegalStateException("Iterator has already been created");
        }
        return iterator;
    }

    @Override
    public Spliterator<GenericRecord> spliterator() {
        return Iterable.super.spliterator();
    }

    /**
     * Returns {@code true} if this collection contains no elements.
     * Prefer this method over {@link #getResultRows()} == 0 because current method reflect actual state of the collection
     * while {@link #getResultRows()} is send from server before sending actual data.
     *
     * @return {@code true} if this collection contains no elements
     */
    public boolean isEmpty() {
        return empty;
    }

    Stream<GenericRecord> stream() {
        return StreamSupport.stream(spliterator(), false);
    }

    /**
     * Returns the metrics of this operation.
     *
     * @return metrics of this operation
     */
    public OperationMetrics getMetrics() {
        return response.getMetrics();
    }

    /**
     * Alias for {@link ServerMetrics#NUM_ROWS_READ}
     * @return number of rows read by server from the storage
     */
    public long getReadRows() {
        return response.getMetrics().getMetric(ServerMetrics.NUM_ROWS_READ).getLong();
    }

    /**
     * Alias for {@link ServerMetrics#NUM_BYTES_READ}
     * @return number of bytes read by server from the storage
     */
    public long getReadBytes() {
        return response.getMetrics().getMetric(ServerMetrics.NUM_BYTES_READ).getLong();
    }

    /**
     * Alias for {@link ServerMetrics#NUM_ROWS_WRITTEN}
     * @return number of rows written by server to the storage
     */
    public long getWrittenRows() {
        return response.getMetrics().getMetric(ServerMetrics.NUM_ROWS_WRITTEN).getLong();
    }

    /**
     * Alias for {@link ServerMetrics#NUM_BYTES_WRITTEN}
     * @return number of bytes written by server to the storage
     */
    public long getWrittenBytes() {
        return response.getMetrics().getMetric(ServerMetrics.NUM_BYTES_WRITTEN).getLong();
    }

    /**
     * Alias for {@link ServerMetrics#ELAPSED_TIME}
     * @return elapsed time in nanoseconds
     */
    public long getServerTime() {
        return response.getMetrics().getMetric(ServerMetrics.ELAPSED_TIME).getLong();
    }

    /**
     * Alias for {@link ServerMetrics#RESULT_ROWS}
     * @return number of returned rows
     */
    public long getResultRows() {
        return response.getMetrics().getMetric(ServerMetrics.RESULT_ROWS).getLong();
    }

    @Override
    public void close() throws Exception {
        response.close();
    }
}

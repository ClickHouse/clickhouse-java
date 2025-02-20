package com.clickhouse.client;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.TimeZone;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataConfig;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseRecord;

/**
 * This encapsulates a server reponse. Depending on concrete implementation, it
 * could be either an in-memory list or a wrapped input stream with
 * {@link com.clickhouse.data.ClickHouseDataProcessor} attached for
 * deserialization. To get data returned from server, depending on actual needs,
 * you have 3 options:
 *
 * <ul>
 * <li>use {@link #records()} or {@link #stream()} to get deserialized
 * {@link ClickHouseRecord} one at a time</li>
 * <li>use {@link #firstRecord()} if you're certain that all you need is the
 * first {@link ClickHouseRecord}</li>
 * <li>use {@link #getInputStream()} or {@link #pipe(OutputStream, int)} if you
 * prefer to handle stream instead of deserialized data</li>
 * </ul>
 */
@Deprecated
public interface ClickHouseResponse extends AutoCloseable, Serializable {
    /**
     * Empty response that can never be closed.
     */
    ClickHouseResponse EMPTY = new ClickHouseResponse() {
        @Override
        public List<ClickHouseColumn> getColumns() {
            return Collections.emptyList();
        }

        @Override
        public ClickHouseResponseSummary getSummary() {
            return ClickHouseResponseSummary.EMPTY;
        }

        @Override
        public ClickHouseInputStream getInputStream() {
            return ClickHouseInputStream.empty();
        }

        @Override
        public Iterable<ClickHouseRecord> records() {
            return Collections.emptyList();
        }

        @Override
        public <T> Iterable<T> records(Class<T> objClass) {
            return Collections.emptyList();
        }

        @Override
        public void close() {
            // do nothing
        }

        @Override
        public boolean isClosed() {
            // ensure the instance is "stateless"
            return false;
        }

        @Override
        public TimeZone getTimeZone() {
            return null;
        }
    };

    /**
     * Gets list of columns.
     *
     * @return non-null list of column
     */
    List<ClickHouseColumn> getColumns();

    /**
     * Gets summary of this response. Keep in mind that the summary may change over
     * time until response is closed.
     *
     * @return non-null summary of this response
     */
    ClickHouseResponseSummary getSummary();

    /**
     * Gets input stream of the response. In general, this is the most
     * memory-efficient way for streaming data from server to client. However, this
     * also means additional work is required for deserialization, especially when
     * using a binary format.
     *
     * @return non-null input stream for getting raw data returned from server
     */
    ClickHouseInputStream getInputStream();

    /**
     * Returns a server timezone if it is returned by server in a header {@code X-ClickHouse-Timezone } or
     * other way. If not, it returns null
     * @return server timezone from server response or null
     */
    default TimeZone getTimeZone() {
        return null;
    }

    /**
     * Gets the first record only. Please use {@link #records()} instead if you need
     * to access the rest of records.
     *
     * @return the first record
     * @throws NoSuchElementException when there's no record at all
     * @throws UncheckedIOException   when failed to read data(e.g. deserialization)
     */
    default ClickHouseRecord firstRecord() {
        return records().iterator().next();
    }

    /**
     * Gets the first record as mapped object. Please use {@link #records(Class)}
     * instead if you need to access the rest of records.
     * 
     * @param <T>      type of the mapped object
     * @param objClass non-null class of the mapped object
     * @return mapped object of the first record
     * @throws NoSuchElementException when there's no record at all
     * @throws UncheckedIOException   when failed to read data(e.g. deserialization)
     */
    default <T> T firstRecord(Class<T> objClass) {
        return records(objClass).iterator().next();
    }

    /**
     * Returns an iterable collection of records which can be walked through in a
     * foreach loop. Please pay attention that: 1) {@link UncheckedIOException}
     * might be thrown when iterating through the collection; and 2) it's not
     * supposed to be called for more than once.
     *
     * @return non-null iterable collection
     * @throws UncheckedIOException when failed to read data(e.g. deserialization)
     */
    Iterable<ClickHouseRecord> records();

    /**
     * Returns an iterable collection of mapped objects which can be walked through
     * in a foreach loop. When {@code objClass} is null or {@link ClickHouseRecord},
     * it's same as calling {@link #records()}.
     *
     * @param <T>      type of the mapped object
     * @param objClass non-null class of the mapped object
     * @return non-null iterable collection
     * @throws UncheckedIOException when failed to read data(e.g. deserialization)
     */
    <T> Iterable<T> records(Class<T> objClass);

    /**
     * Pipes the contents of this response into the given output stream. Keep in
     * mind that it's caller's responsibility to flush and close the output stream.
     *
     * @param output     non-null output stream, which will remain open
     * @param bufferSize buffer size, 0 or negative value will be treated as
     *                   {@link ClickHouseClientOption#BUFFER_SIZE}
     * @throws IOException when error occurred reading or writing data
     */
    default void pipe(OutputStream output, int bufferSize) throws IOException {
        ClickHouseInputStream.pipe(getInputStream(), ClickHouseChecker.nonNull(output, "output"),
                ClickHouseDataConfig.getBufferSize(bufferSize,
                        (int) ClickHouseClientOption.BUFFER_SIZE.getDefaultValue(),
                        (int) ClickHouseClientOption.MAX_BUFFER_SIZE.getDefaultValue()));
    }

    /**
     * Gets stream of records to process.
     *
     * @return stream of records
     */
    default Stream<ClickHouseRecord> stream() {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(records().iterator(),
                Spliterator.IMMUTABLE | Spliterator.NONNULL | Spliterator.ORDERED), false);
    }

    /**
     * Gets stream of mapped objects to process.
     *
     * @return stream of mapped objects
     */
    default <T> Stream<T> stream(Class<T> objClass) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(records(objClass).iterator(),
                Spliterator.IMMUTABLE | Spliterator.NONNULL | Spliterator.ORDERED), false);
    }

    @Override
    void close();

    /**
     * Checks whether the response has been closed or not.
     *
     * @return true if the response has been closed; false otherwise
     */
    boolean isClosed();
}

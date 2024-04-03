package com.clickhouse.client.api.data_formats;

import com.clickhouse.data.ClickHouseRecord;

import java.util.function.Consumer;

public interface RecordReader {

    /**
     * Read a batch of records from a stream.
     *
     * @param size the maximum number of records to read
     * @param consumer the consumer to process the records
     * @param errorHandler the consumer to handle exceptions
     * @return true if there are more records to read, false otherwise
     */
    boolean readBatch(int size, Consumer<ClickHouseRecord> consumer, Consumer<Exception> errorHandler);
}

package com.clickhouse.client.api.query;

import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.data_formats.RowBinaryWithNamesAndTypesFormatReader;
import com.clickhouse.client.api.data_formats.internal.BinaryReaderBackedRecord;
import com.clickhouse.data.ClickHouseFormat;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Records implements Iterable<GenericRecord> {

    private final QueryResponse response;

    private final ClickHouseBinaryFormatReader reader;

    private boolean empty;

    public Records(QueryResponse response, QuerySettings finalSettings) {
        this.response = response;
        if (!response.getFormat().equals(ClickHouseFormat.RowBinaryWithNamesAndTypes)) {
            throw new ClientException("Unsupported format: " + finalSettings.getFormat());
        }
        this.reader = new RowBinaryWithNamesAndTypesFormatReader(response.getInputStream());
        this.empty = !reader.hasNext();
    }

    @Override
    public Iterator<GenericRecord> iterator() {
        return new Iterator<>() {

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
    }

    @Override
    public Spliterator<GenericRecord> spliterator() {
        return Iterable.super.spliterator();
    }

    int size() {
        return 0;
    }

    /**
     * Returns {@code true} if this collection contains no elements.
     *
     * @return {@code true} if this collection contains no elements
     */
    boolean isEmpty() {
        return empty;
    }

    Stream<GenericRecord> stream() {
        return StreamSupport.stream(spliterator(), false);
    }
}

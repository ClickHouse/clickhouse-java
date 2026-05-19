package com.clickhouse.client.api.data_formats;

/**
 * Reader for ClickHouse <b>text</b> output formats (such as
 * {@code JSONEachRow}).
 *
 * <p>Row navigation, schema access, and typed accessors are inherited from
 * {@link ClickHouseFormatReader}; this interface specializes the contract for
 * text-encoded result streams</p>
 *
 * <p>Implementation of a reader may not support every accessor declared on
 * {@link ClickHouseFormatReader}; unsupported accessors are expected to throw
 * {@link UnsupportedOperationException}.</p>
 */
public interface ClickHouseTextFormatReader extends ClickHouseFormatReader {

    String currentRowAsString();

    Object currentRowAsObject();
}

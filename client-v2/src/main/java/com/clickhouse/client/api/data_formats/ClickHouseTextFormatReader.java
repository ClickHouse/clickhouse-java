package com.clickhouse.client.api.data_formats;

/**
 * Reader for ClickHouse <b>text</b> output formats (such as
 * {@code JSONEachRow}).
 *
 * <p>Row navigation, schema access, and typed accessors are inherited from
 * {@link ClickHouseFormatReader}; this interface specializes the contract for
 * text-encoded result streams and is the type returned by the text factory
 * method on {@link com.clickhouse.client.api.Client}. Readers for binary
 * output formats implement {@link ClickHouseBinaryFormatReader} instead.</p>
 *
 * <p>Instances are produced by
 * {@link com.clickhouse.client.api.Client#newTextFormatReader(com.clickhouse.client.api.query.QueryResponse)}.
 * Concrete readers may not support every accessor declared on
 * {@link ClickHouseFormatReader}; unsupported accessors are expected to throw
 * {@link UnsupportedOperationException}.</p>
 */
public interface ClickHouseTextFormatReader extends ClickHouseFormatReader {
}

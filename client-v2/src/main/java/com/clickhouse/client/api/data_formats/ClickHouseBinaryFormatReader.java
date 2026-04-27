package com.clickhouse.client.api.data_formats;

/**
 * Reader for ClickHouse <b>binary</b> output formats (such as {@code Native},
 * {@code RowBinary}, {@code RowBinaryWithNames}, and
 * {@code RowBinaryWithNamesAndTypes}).
 *
 * <p>Row navigation, schema access, and typed accessors are inherited from
 * {@link ClickHouseFormatReader}; this interface specializes the contract for
 * binary-encoded result streams and is the type returned by the binary
 * factory methods on {@link com.clickhouse.client.api.Client}. Readers for
 * text-oriented output formats (for example {@code JSONEachRow}) implement
 * {@link ClickHouseTextFormatReader} instead.</p>
 *
 * <p>Instances are produced by
 * {@link com.clickhouse.client.api.Client#newBinaryFormatReader(com.clickhouse.client.api.query.QueryResponse)}
 * and
 * {@link com.clickhouse.client.api.Client#newBinaryFormatReader(com.clickhouse.client.api.query.QueryResponse, com.clickhouse.client.api.metadata.TableSchema)}.</p>
 */
public interface ClickHouseBinaryFormatReader extends ClickHouseFormatReader {
}

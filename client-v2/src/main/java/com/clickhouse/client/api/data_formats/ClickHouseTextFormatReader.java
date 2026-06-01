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
 *
 * <h2>Format-specific accessor behavior</h2>
 *
 * <p>Text formats encode values as strings rather than as typed binary tokens,
 * so a number of accessors inherited from {@link ClickHouseFormatReader} have
 * looser or otherwise text-specific semantics:</p>
 *
 * <ul>
 *     <li><b>{@code Enum8} / {@code Enum16}.</b> ClickHouse always emits enum
 *     columns in text formats (including {@code JSONEachRow}) as their
 *     <em>string label</em>, never as the underlying numeric value. As a
 *     result, the inherited {@link ClickHouseFormatReader#getEnum8(String)
 *     getEnum8} / {@link ClickHouseFormatReader#getEnum16(String) getEnum16}
 *     accessors are not usable for real enum data and are expected to throw
 *     a {@link RuntimeException}. Callers should read enum columns with
 *     {@link ClickHouseFormatReader#getString(String) getString} instead.</li>
 *     <li><b>Temporal and other parse-on-read accessors</b>
 *     ({@link ClickHouseFormatReader#getLocalDate(String) getLocalDate},
 *     {@link ClickHouseFormatReader#getLocalTime(String) getLocalTime},
 *     {@link ClickHouseFormatReader#getLocalDateTime(String) getLocalDateTime},
 *     {@link ClickHouseFormatReader#getOffsetDateTime(String) getOffsetDateTime},
 *     {@link ClickHouseFormatReader#getUUID(String) getUUID}) parse the value
 *     out of its textual representation, so they fail with a
 *     {@link RuntimeException} when the underlying value is not a valid
 *     literal for the requested type.</li>
 *     <li><b>Schema inference</b> in text formats may be best-effort: when the
 *     reader cannot determine the original ClickHouse type from the wire
 *     content, the inferred {@code TableSchema} may report a coarser type
 *     than a binary reader would for the same column.</li>
 * </ul>
 */
public interface ClickHouseTextFormatReader extends ClickHouseFormatReader {
}

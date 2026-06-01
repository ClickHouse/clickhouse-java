package com.clickhouse.client.api.data_formats;

import java.util.Map;

/**
 * Interface for JSON row processors.
 *
 * <p>Implementations are responsible for reading one JSON object per call
 * from the underlying input stream and returning its fields as a
 * {@link Map} keyed by column name.</p>
 *
 * <h2>Iteration-order contract</h2>
 *
 * <p>The reader infers its column schema from the keys of the <b>first</b>
 * row, in iteration order, and uses that order to resolve 1-based column
 * indexes for every subsequent row. Implementations <b>must</b> therefore
 * return a {@link Map} that preserves the insertion order of the JSON keys
 * as they appeared on the wire (e.g. {@link java.util.LinkedHashMap}); a
 * hash-ordered {@link java.util.HashMap} is not acceptable because it
 * destabilises index-based access.</p>
 *
 * <p>The bundled {@link JacksonJsonParserFactory} (which produces
 * {@link java.util.LinkedHashMap} via Jackson's {@code Map.class} default)
 * and {@link GsonJsonParserFactory} (which produces {@code LinkedTreeMap})
 * both satisfy this contract. Custom implementations must take care not to
 * regress it.</p>
 */
public interface JsonParser extends AutoCloseable {


    /**
     * Reads next row from the input stream.
     *
     * <p>The returned map must preserve the insertion order of the JSON
     * keys as defined by the {@linkplain JsonParser interface contract}.</p>
     *
     * @return map of column names to values, or null if no more rows
     * @throws Exception if an error occurs during parsing
     */
    Map<String, Object> nextRow() throws Exception;
}
